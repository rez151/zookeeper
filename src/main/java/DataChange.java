import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * Created by reserchr on 17.06.17.
 */
public class DataChange implements Watcher {
    private ZooKeeper zk;
    private Watcher dataWatcher = new Watcher() {
        public void process(WatchedEvent watchedEvent) {
            System.out.println("Data Watcher = " + watchedEvent);
            try {
                getMyData();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    };

    private AsyncCallback.DataCallback dataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int returnCode, String path,
                                  Object context, byte[] data, Stat stat) {
            String serviceSocket = new String(data);
            System.out.println("Service Socket = " + serviceSocket);
        }
    };

    public DataChange() throws IOException, KeeperException, InterruptedException {
        String zooConnect = "192.168.162.128:2181";
        zk = new ZooKeeper(zooConnect, 15000, this);
        getMyData();
    }

    private void getMyData() throws KeeperException, InterruptedException {
        zk.getData("/MyThriftService", dataWatcher, dataCallback, null);
    }

    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) {
        try {
            new DataChange();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        while (true) {
        }
    }
}

