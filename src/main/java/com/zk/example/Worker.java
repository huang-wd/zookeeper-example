package com.zk.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

/**
 * @author huangweidong
 * @date 2018/11/21
 */
@Slf4j
public class Worker implements Watcher {

    private Random random = new Random(this.hashCode());


    ZooKeeper zk;
    String hostPort;
    String serverId = Integer.toHexString(random.nextInt());

    Worker(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public void process(WatchedEvent event) {
        log.info("{}, {}", event.toString(), hostPort);
    }

    String name;

    void register() {
        name = "worker-" + serverId;
        zk.create("/workers/" + name,
                "Idle".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback,
                null
        );
    }

    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    register();
                    break;
                case OK:
                    log.info("Registered successfully: {}", serverId);
                    break;
                case NODEEXISTS:
                    log.warn("Already registered: {}", serverId);
                    break;
                default:
                    log.error("Something went wrong: ", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    AsyncCallback.StatCallback statusUpdateCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    updateStatus((String) ctx);
                    return;
            }
        }
    };

    String status;

    synchronized private void updateStatus(String status) {
        if (status == this.status) {
            zk.setData("/workers/" + name, status.getBytes(), -1, statusUpdateCallback, status);
        }
    }

    public static void main(String[] args) throws Exception {
        String hostPort = "172.18.2.129:2181";
        Worker w = new Worker(hostPort);
        w.startZK();
        w.register();
        Thread.sleep(30000);
    }
}
