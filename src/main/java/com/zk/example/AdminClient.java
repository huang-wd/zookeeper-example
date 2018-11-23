package com.zk.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Date;

/**
 * @author huangweidong
 * @date 2018/11/21
 */
@Slf4j
public class AdminClient implements Watcher {

    ZooKeeper zk;
    String hostPort;

    public AdminClient(String hostPort) {
        this.hostPort = hostPort;
    }

    void start() throws Exception {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void listState() throws Exception {
        try {
            Stat stat = new Stat();
            byte[] masterData = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            log.info("Master: {} since {}", new String(masterData), startDate);
        } catch (KeeperException.NoNodeException e) {
            log.info("No master");
        }

        log.info("Workers:");
        for (String w : zk.getChildren("/workers", false)) {
            byte[] data = zk.getData("/workers" + w, false, null);
            String state = new String(data);
            log.info("\t {}: {}", w, state);
        }

        log.info("Tasks:");
        for (String t : zk.getChildren("/assign", false)) {
            log.info("\t{}", t);
        }
    }

    public void process(WatchedEvent event) {
        log.info("{}", event);
    }

    public static void main(String[] args) throws Exception {
        String hostPort = "172.18.2.129:2181";
        AdminClient c = new AdminClient(hostPort);
        c.start();
        c.listState();
    }
}
