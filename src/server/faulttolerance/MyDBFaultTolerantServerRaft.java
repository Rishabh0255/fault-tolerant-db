package server.faulttolerance;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.ReplicatedServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.io.File;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.json.JSONObject;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;

/**
 * PA4.3 Optional Extra Credit: Fault-tolerant database using Apache Ratis
 * 
 * This uses Apache Ratis (Raft consensus protocol) for replication instead of GigaPaxos or Zookeeper
 */
public class MyDBFaultTolerantServerRaft extends server.MyDBSingleServer {

    public static final int SLEEP = 1000;
    public static final boolean DROP_TABLES_AFTER_TESTS = true;
    public static final int MAX_LOG_SIZE = 400;

    private Session cassandraSession;
    private Cluster cassandraCluster;
    private String keyspace;
    private RaftServer raftServer;
    private DatabaseStateMachine stateMachine;
    private RaftGroupId raftGroupId;
    private Map<Long, InetSocketAddress> pendingRequests = new ConcurrentHashMap<>();
    private long requestID = 0;

    /**
     * State Machine that executes CQL on Cassandra
     */
    private class DatabaseStateMachine extends BaseStateMachine {
        private int executedCount = 0;

        @Override
        public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage storage) throws IOException {
            super.initialize(server, groupId, storage);
        }

        @Override
        public long takeSnapshot() {
            return executedCount;
        }

        @Override
        public TransactionContext applyTransactionSerial(TransactionContext transaction) {
            RaftProtos.LogEntryProto entry = transaction.getLogEntry();
            ByteString data = entry.getStateMachineLogEntry().getLogData();
            
            try {
                String json = data.toStringUtf8();
                JSONObject obj = new JSONObject(json);
                long rid = obj.getLong("rid");
                String cmd = obj.getString("cmd");
                
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
                    } 
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {

                e.printStackTrace();
            }
            
            return transaction;
        }
    }

    public MyDBFaultTolerantServerRaft(NodeConfig<String> nodeConfig, String myID, InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID), nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET), isaDB, myID);

        this.keyspace = myID;
        
        try {
            cassandraCluster = Cluster.builder().addContactPoint(isaDB.getHostString()).withPort(isaDB.getPort()).build();
            cassandraSession = cassandraCluster.connect();
            cassandraSession.execute(
                "CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
            );
            cassandraSession.execute("USE " + keyspace);
            
            setupRaftServer(nodeConfig, myID);
            
        } catch (Exception e) {

            throw new IOException("Failed to initialize", e);
        }
    }

    private void setupRaftServer(NodeConfig<String> nodeConfig, String myID) throws IOException {
        List<RaftPeer> peers = new ArrayList<>();
        
        for (String serverId : Arrays.asList("server0", "server1", "server2")) {
            String address = nodeConfig.getNodeAddress(serverId).getHostAddress();
            int port = nodeConfig.getNodePort(serverId);
            
            RaftPeer peer = RaftPeer.newBuilder().setId(RaftPeerId.valueOf(serverId)).setAddress(address + ":" + port).build();
            peers.add(peer);
        }
        
        RaftGroupId groupId = RaftGroupId.valueOf(UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1"));
        this.raftGroupId = groupId; 
        RaftGroup raftGroup = RaftGroup.valueOf(groupId, peers);
        
        RaftProperties properties = new RaftProperties();
        RaftServerConfigKeys.setStorageDir(properties, 
            Collections.singletonList(new File("./raft_logs/" + myID)));
        
        int raftPort = nodeConfig.getNodePort(myID);
        GrpcConfigKeys.Server.setPort(properties, raftPort);
        
        
        RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(300, java.util.concurrent.TimeUnit.MILLISECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(500, java.util.concurrent.TimeUnit.MILLISECONDS));
        
        stateMachine = new DatabaseStateMachine();
        
        raftServer = RaftServer.newBuilder()
            .setServerId(RaftPeerId.valueOf(myID))
            .setGroup(raftGroup)
            .setProperties(properties)
            .setStateMachine(stateMachine)
            .build();
            
        raftServer.start();
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        try {
            String cmd = new String(bytes, StandardCharsets.UTF_8);
            long rid = requestID++;
            pendingRequests.put(rid, header.sndr);
            
            JSONObject obj = new JSONObject();
            obj.put("rid", rid);
            obj.put("cmd", cmd);
            
            ByteString data = ByteString.copyFrom(obj.toString().getBytes(StandardCharsets.UTF_8));
            
            RaftClientRequest request = RaftClientRequest.newBuilder()
                .setClientId(ClientId.randomId())
                .setServerId(raftServer.getId())
                .setGroupId(raftGroupId)
                .setCallId(rid)
                .setMessage(Message.valueOf(data))
                .setType(RaftClientRequest.writeRequestType())
                .build();
            
            raftServer.submitClientRequestAsync(request);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        // Ratis handles server-to-server communication
    }

    @Override
    public void close() {
        try {
            if (raftServer != null) raftServer.close();

            if (cassandraSession != null) cassandraSession.close();

            if (cassandraCluster != null) cassandraCluster.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        super.close();
    }

    public static void main(String[] args) throws IOException {
        new MyDBFaultTolerantServerRaft(
            NodeConfigUtils.getNodeConfigFromFile(args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer.SERVER_PORT_OFFSET),
            args[1],
            args.length > 2 ? Util.getInetSocketAddressFromString(args[2]) 
                : new InetSocketAddress("localhost", 9042)
        );
    }
}