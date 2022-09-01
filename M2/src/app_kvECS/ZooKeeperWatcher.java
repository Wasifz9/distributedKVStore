package app_kvECS;

import java.io.PrintWriter;
import java.io.StringWriter;

import java.util.List;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.*;

import org.apache.log4j.Logger;

import ecs.IECSNode.NodeEvent;

import app_kvECS.ECS;

import logger.LogSetup;

public class ZooKeeperWatcher implements Watcher {

    private static Logger logger = Logger.getRootLogger();

    private ECS caller = null;

    public ZooKeeperWatcher(ECS caller) {
        this.caller = caller;
    }

    @Override
    public void process(final WatchedEvent event) {
        if (event == null) {
            return;
        }

        // Run in separate thread to avoid blocking the watcher...
        Thread eventThread = new Thread(new Runnable() {
            public void run() {
                Thread.currentThread().setName("ZK_Watcher" + Thread.currentThread().getName());
                // Get connection status
                KeeperState keeperState = event.getState();
                // Event type
                EventType eventType = event.getType();
                // Affected path
                String path = event.getPath();

                logger.info("Event from:\t" + path);
                logger.info("Connection status:\t" + keeperState.toString());
                logger.info("Event type:\t" + eventType.toString());

                switch (eventType) {
                    case None:
                        logger.info("Successfully connected to ZK server!");
                        break;
                    case NodeDataChanged:
                        try {
                            logger.info("Node data update");
                            // Resubscribe below:
                            logger.info("Resubscribing back to " + path);
                            byte[] dataBytes = caller._zooKeeper.getData(path,
                                    true, null);
                            String recv = new String(dataBytes,
                                    "UTF-8");
                            logger.info("ZooKeeper Notification:" + recv);
                            String[] reqs = recv.split("~~");

                            // There may be piggybacked requests
                            for (String req : reqs) {
                                String[] data = req.split("~");
                                switch (NodeEvent.valueOf(data[0])) {
                                    case COPY_COMPLETE:
                                        caller.completeCopy(path);
                                        break;
                                    case CRASH_COMPLETE:
                                        caller.crashComplete(path);
                                        break;
                                    // Skip the following events:
                                    case START:
                                    case BOOT:
                                    case METADATA:
                                    case STOP:
                                    case COPY:
                                    case MOVE:
                                    case SHUTDOWN:
                                    case CRASH:
                                        break;
                                    default:
                                        logger.error("Unrecognized node event:" + data[0]);
                                }
                            }
                        } catch (Exception e) {
                            logger.error("Error while getting data");
                            exceptionLogger(e);
                        }
                        break;
                    case NodeChildrenChanged:
                        // Is it a new child? Or did a node get deleted?
                        logger.info("Node created/deleted");
                        caller.nodeRemovedCreated();

                        // Resubscribe back:
                        try {
                            logger.info("Resubscribing back to children");
                            caller._zooKeeper.getChildren(caller._rootZnode,
                                    true);
                        } catch (Exception e) {
                            logger.error("Error while resubscribing back to obtain children");
                            exceptionLogger(e);
                        }
                        break;
                    default:
                        break;
                }
            }
        });
        eventThread.start();
    }

    private void exceptionLogger(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        logger.error(sw.toString());
    }
}
