package com.zk.example;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * @author huangweidong
 * @date 2018/11/19
 */
public class Master implements Watcher {

    Random random = new Random();

    ZooKeeper zk;
    String hostPort;

    String serverId = Integer.toHexString(random.nextInt());

    static boolean isLeader = false;

    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case OK:
                    isLeader = true;
                    break;
                default:
                    isLeader = false;
            }
            System.out.println("I'm " + (isLeader ? "" : "not ") + "the leader");
        }
    };

    AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case NONODE:
                    runForMaster();
                    return;
            }
        }
    };

    AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path, (byte[]) ctx);
                    break;
                case OK:
                    System.out.println("Parent created");
                    break;
                case NODEEXISTS:
                    System.out.println("Parent already registered: " + path);
                    break;
                default:
                    System.out.println("Something went wrong:");
                    KeeperException.create(KeeperException.Code.get(rc), path).printStackTrace();
            }
        }
    };

    Master(String hostPort) {
        this.hostPort = hostPort;
    }

    void checkMaster() {
        zk.getData("/master", false, masterCheckCallback, null);
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZK() throws InterruptedException {
        zk.close();
    }

    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    void runForMaster() {
        zk.create("/master", serverId.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

    public void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    void createParent(String path, byte[] data) {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
    }

    public static void main(String[] args) throws Exception {
        String hostPort = "172.18.2.129:2181";
        Master m = new Master(hostPort);
        m.startZK();
        m.runForMaster();
        Thread.sleep(60000);
        m.stopZK();
    }

}
