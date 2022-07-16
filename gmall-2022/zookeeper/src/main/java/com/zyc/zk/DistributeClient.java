package com.zyc.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributeClient {
    private String connectString = "hadoop102:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zk;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DistributeClient client = new DistributeClient();
        // 1.获取zk连接
        client.getConnect();
        // 2.监听servers下面节点的增加和删除
        client.getServerList();
        // 3. 业务逻辑
        client.bussiness();
    }

    private void bussiness() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getServerList() throws InterruptedException, KeeperException {
        List<String> childen = zk.getChildren("/servers", true);

        ArrayList<String> servers = new ArrayList<>();
        for (String child : childen) {
            byte[] data = zk.getData("/servers/" + child, false, null);

            servers.add(new String(data));
        }

        System.out.println(servers);
    }

    private void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    getServerList();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
