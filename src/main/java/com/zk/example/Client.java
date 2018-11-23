package com.zk.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;

import java.io.IOException;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * @author huangweidong
 * @date 2018/11/21
 */
@Slf4j
public class Client implements Watcher {
    ZooKeeper zk;
    String hostPort;

    Client(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    String queueCommand(String command) {
        while (true) {
            try {
                String name = zk.create("/tasks/task-",
                        command.getBytes(),
                        OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL);
                return name;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }
    }

    public void process(WatchedEvent event) {
        log.info("{}", event);
    }

    public static void main(String[] args) throws Exception {
        String hostPort = "172.18.2.129:2181";
        Client c = new Client(hostPort);
        c.startZK();
    }
}
