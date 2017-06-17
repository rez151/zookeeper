import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by reserchr on 17.06.17.
 */

public class ClusterStatus implements LeaderLatchListener {

    // zookeeper configuration fields
    ZooKeeper zk;

    String nodeId;
    boolean closed;

    private CuratorFramework client;

    private LeaderLatch leaderLatch;

    public ClusterStatus(String nodeId) throws IOException {
        this.nodeId = nodeId;
        this.closed = false;

        String zooConnect = "192.168.162.128:2181";
        // zk = new ZooKeeper(zooConnect, 15000, this);

        client = CuratorFrameworkFactory.newClient(zooConnect, new ExponentialBackoffRetry(1000, 3));
        client.start();

        try {
            client.blockUntilConnected();

            leaderLatch = new LeaderLatch(client, "/leader", nodeId);
            leaderLatch.addListener(this);
            leaderLatch.start();
        } catch (Exception e) {
            System.out.println("Error when starting leaderLatch");
        }
    }

    public boolean hasLeadership() {
        return leaderLatch.hasLeadership();
    }

    @Override
    public void isLeader() {
        System.out.println("Node : " + nodeId + " is a leader");
    }

    @Override
    public void notLeader() {
        System.out.println("Node : " + nodeId + " is not a leader");
    }

    public void close() {
        try {
            System.out.println("closing " + nodeId);
            leaderLatch.close();
            closed = true;
        } catch (IOException e) {
            System.out.println("Error when closing leaderLatch.");
        }
        client.close();
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public boolean isClosed() {
        return closed;
    }

    public void setClosed(boolean closed) {
        this.closed = closed;
    }

    public static void main(String[] args) throws InterruptedException {

        ArrayList<ClusterStatus> nodes = new ArrayList<ClusterStatus>();

        try {
            nodes.add(new ClusterStatus("0"));
            nodes.add(new ClusterStatus("1"));
            nodes.add(new ClusterStatus("2"));
            nodes.add(new ClusterStatus("3"));
        } catch (IOException e) {
            e.printStackTrace();
        }


        boolean allClosed = false;
        while (!allClosed) {
            allClosed = true;
            for (ClusterStatus node : nodes) {
                if (node.hasLeadership()) {
                    Thread.sleep(100);
                    node.close();
                }
                if(!node.isClosed())
                    allClosed = false;
            }
        }
        System.out.println("all stati closed");
    }
}