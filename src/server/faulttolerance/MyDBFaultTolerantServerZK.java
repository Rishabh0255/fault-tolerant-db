package server.faulttolerance;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.AVDBReplicatedServer;
import server.ReplicatedServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;

/**
 * This class should implement your replicated fault-tolerant database server if
 * you wish to use Zookeeper or other custom consensus protocols to order client
 * requests.
 * <p>
 * Refer to {@link server.ReplicatedServer} for a starting point for how to do
 * server-server messaging or to {@link server.AVDBReplicatedServer} for a
 * non-fault-tolerant replicated server.
 * <p>
 * You can assume that a single *fault-tolerant* Zookeeper server at the default
 * host:port of localhost:2181 and you can use this service as you please in the
 * implementation of this class.
 * <p>
 * Make sure that both a single instance of Cassandra and a single Zookeeper
 * server are running on their default ports before testing.
 * <p>
 * You can not store in-memory information about request logs for more than
 * {@link #MAX_LOG_SIZE} requests.
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer {

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 */
	public static final int SLEEP = 1000;

	/**
	 * Set this to true if you want all tables drpped at the end of each run
	 * of tests by GraderFaultTolerance.
	 */
	public static final boolean DROP_TABLES_AFTER_TESTS=true;

	/**
	 * Maximum permitted size of any collection that is used to maintain
	 * request-specific state, i.e., you can not maintain state for more than
	 * MAX_LOG_SIZE requests (in memory or on disk). This constraint exists to
	 * ensure that your logs don't grow unbounded, which forces
	 * checkpointing to
	 * be implemented.
	 */
	public static final int MAX_LOG_SIZE = 400;

	public static final int DEFAULT_PORT = 2181;

	// Zookeeper connection and state
	private ZooKeeper zk;
	private Session cassandraSession;
	private Cluster cassandraCluster;
	private String keyspace;
	private Map<Long, InetSocketAddress> pendingRequests = new ConcurrentHashMap<>();
	private long requestID = 0;
	private int executedCount = 0;
	private String lastExecuted = null;

	/**
	 * @param nodeConfig Server name/address configuration information read
	 *                      from
	 *                   conf/servers.properties.
	 * @param myID       The name of the keyspace to connect to, also the name
	 *                   of the server itself. You can not connect to any other
	 *                   keyspace if using Zookeeper.
	 * @param isaDB      The socket address of the backend datastore to which
	 *                   you need to establish a session.
	 * @throws IOException
	 */
	public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String
			myID, InetSocketAddress isaDB) throws IOException {
		super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
				nodeConfig.getNodePort(myID) - ReplicatedServer
						.SERVER_PORT_OFFSET), isaDB, myID);

		// TODO: Make sure to do any needed crash recovery here.
		this.keyspace = myID;
		
		try {
			//connect to cassandra
			cassandraCluster = Cluster.builder().addContactPoint(isaDB.getHostString()).withPort(isaDB.getPort()).build();
			cassandraSession = cassandraCluster.connect();
			cassandraSession.execute(
				"CREATE KEYSPACE IF NOT EXISTS " + keyspace +
				" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
			);
			cassandraSession.execute("USE " + keyspace);
			
			// connect to Zookeeper
			CountDownLatch latch = new CountDownLatch(1);
			zk = new ZooKeeper("localhost:2181", 30000, event -> {
				if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
					latch.countDown();
				}
			});

			latch.await();
			
			//Ensure znodes exist
			try {
				zk.create("/proposals", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException.NodeExistsException e) {}
			
			try {
				zk.create("/checkpoint", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException.NodeExistsException e) {}
			
			//Crash recovery
			Stat stat = zk.exists("/checkpoint", false);
			if (stat != null && stat.getDataLength() > 0) {
				byte[] data = zk.getData("/checkpoint", false, null);
				if (data != null && data.length > 0) {
					String s = new String(data, StandardCharsets.UTF_8);
					JSONObject cp = new JSONObject(s);
					executedCount = cp.getInt("count");
					lastExecuted = cp.optString("last", null);
				}
			}
			
			//Set up watcher 1st to avoid missing proposals
			Watcher proposalWatcher = new Watcher() {
				public void process(WatchedEvent event) {
					if(event.getType() == Event.EventType.NodeChildrenChanged) {
						try {
							List<String> ps = zk.getChildren("/proposals", this);
							Collections.sort(ps);
							for (String p: ps) {
								if (lastExecuted != null && p.compareTo(lastExecuted) <= 0) continue;
								byte[] d = zk.getData("/proposals/" + p, false, null);
								if (d != null && d.length > 0) {
									String str = new String(d, StandardCharsets.UTF_8);
									JSONObject o = new JSONObject(str);
									long rid = o.getLong("rid");
									String cmd = o.getString("cmd");

									if (!cmd.trim().startsWith("{")) {
										cassandraSession.execute(cmd);
										executedCount++;
									}

									if (pendingRequests.containsKey(rid)) {
										InetSocketAddress addr = pendingRequests.remove(rid);
										JSONObject resp = new JSONObject();
										resp.put("rid", rid);
										resp.put("status", "ok");

										try {
											clientMessenger.send(addr, resp.toString().getBytes(StandardCharsets.UTF_8));
										
										} catch (IOException ex) {}
									}

									lastExecuted = p;

									if (executedCount > 0 && executedCount % 100 == 0) {
										JSONObject cp = new JSONObject();
										cp.put("count", executedCount);
										cp.put("last", lastExecuted);
										byte[] cpData = cp.toString().getBytes(StandardCharsets.UTF_8);
										zk.setData("/checkpoint", cpData, -1);
									}
								}
							}

							zk.getChildren("/proposals", this);

						} catch (Exception e) {}

					}
				}
			};

			List<String> initialProposals = zk.getChildren("/proposals", proposalWatcher);
			Collections.sort(initialProposals);
			
			//process any proposals we missed
			for(String p: initialProposals){
				if (lastExecuted != null && p.compareTo(lastExecuted) <= 0) continue;

				byte[] data = zk.getData("/proposals/" + p, false, null);
				if(data != null && data.length > 0){
					String s = new String(data, StandardCharsets.UTF_8);
					JSONObject obj = new JSONObject(s);
					String cmd = obj.getString("cmd");

					if (!cmd.trim().startsWith("{")) {
						cassandraSession.execute(cmd);
						executedCount++;
					}

					lastExecuted = p;
				}
			}
			
		} catch (Exception e) {
			throw new IOException(e);

		}
	}

	/**
	 * TODO: process bytes received from clients here.
	 */
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
		//create proposal in Zookeeper with sequential node

		try {
			String cmd = new String(bytes, StandardCharsets.UTF_8);
			long rid = requestID++;
			pendingRequests.put(rid, header.sndr);
			
			JSONObject obj = new JSONObject();
			obj.put("rid", rid);
			obj.put("cmd", cmd);
			
			zk.create(
				"/proposals/prop-",
				obj.toString().getBytes(StandardCharsets.UTF_8),
				ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT_SEQUENTIAL
			);
		} catch (Exception e) {}

	}

	/**
	 * TODO: process bytes received from fellow servers here.
	 */
	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
		// Not needed
	}


	/**
	 * TODO: Gracefully close any threads or messengers you created.
	 */
	public void close() {
		try {
			if(zk != null) zk.close();
			if (cassandraSession != null) cassandraSession.close();
			if (cassandraCluster != null) cassandraCluster.close();

		} catch (Exception e) {}

		super.close();

	}

	public static enum CheckpointRecovery {
		CHECKPOINT, RESTORE;
	}

	/**
	 * @param args args[0] must be server.properties file and args[1] must be
	 *             myID. The server prefix in the properties file must be
	 *             ReplicatedServer.SERVER_PREFIX. Optional args[2] if
	 *             specified
	 *             will be a socket address for the backend datastore.
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		new MyDBFaultTolerantServerZK(NodeConfigUtils.getNodeConfigFromFile
				(args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer
						.SERVER_PORT_OFFSET), args[1], args.length > 2 ? Util
				.getInetSocketAddressFromString(args[2]) : new
				InetSocketAddress("localhost", 9042));
	}

}