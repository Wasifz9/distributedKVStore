package app_kvServer;

import java.io.StringWriter;
import java.io.PrintWriter;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import org.apache.log4j.Logger;

import app_kvServer.IKVServer.Status;
import ecs.IECSNode.NodeEvent;
import logger.LogSetup;

public class ZooKeeperWatcher implements Watcher {

    private static Logger logger = Logger.getRootLogger();

    private KVServer caller = null;

    public ZooKeeperWatcher(KVServer caller) {
        this.caller = caller;
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("WATCHER NOTIFICATION (from storage server)!");
        if (event == null) {
            return;
        }

        // Get connection status
        KeeperState keeperState = event.getState();
        // Event type
        EventType eventType = event.getType();
        // Affected path
        String path = event.getPath();

        logger.info("Connection status:\t" + keeperState.toString());
        logger.info("Event type:\t" + eventType.toString());

        switch (eventType) {
            case None:
                if (keeperState == KeeperState.Closed) {
                    return;
                } else {
                    logger.info("Successfully connected to ZK server!");
                }
                try {
                    // Since notifications are a one time thing, we must reset the watcher
                    String watchPath = String.format("%s/%s", caller._rootZnode, caller.name);
                    logger.info("Resetting watchers on " + watchPath + " ...");
                    caller._zooKeeper.getData(watchPath, this, null);
                } catch (Exception e) {
                    logger.error("Error while resetting watcher!");
                    logger.error(e.getMessage());
                }
                break;
            case NodeDeleted:
                caller.shutDown();
                break;
            case NodeDataChanged:
                try {
                    // Get data + resubscribe back to watcher
                    byte[] dataBytes = caller._zooKeeper.getData(path,
                            true, null);
                    String recv = new String(dataBytes,
                            "UTF-8");
                    logger.info("ZooKeeper Notification:" + recv);
                    String[] reqs = recv.split("~~");

                    // Loop in case there are piggyback requests
                    for (String req : reqs) {
                        String[] data = req.split("~");
                        switch (NodeEvent.valueOf(data[0])) {
                            case METADATA:
                                caller.loadMetadata(data[1]);
                                break;
                            case START:
                                caller.start();
                                break;
                            case STOP:
                                caller.stop();
                                break;
                            case COPY:
                                String[] b = data[1].split(",");
                                String[] range = { b[0], b[1] };
                                caller.moveData(range, b[2]);
                                break;
                            case CRASH:
                                caller.crash(data[1]);
                                break;
                            case MOVE:
                                caller.completeMove();
                                break;
                            case SHUTDOWN:
                                caller.reconcileData(true);
                                caller.shutDown();
                                break;
                            // Ignored events:
                            case BOOT:
                            case COPY_COMPLETE:
                            case CRASH_COMPLETE:
                                break;
                            default:
                                logger.error("Unrecognized node event:" + data[0]);
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error while getting data");

                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    e.printStackTrace(pw);
                    logger.error(sw.toString());
                }
                break;
        }
    }

}
