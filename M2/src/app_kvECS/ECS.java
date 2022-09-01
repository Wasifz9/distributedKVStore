package app_kvECS;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;

import java.util.Enumeration;
import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Collection;
import java.util.Scanner;
import java.util.Date;
import java.util.Queue;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.HashMap;
import java.util.LinkedList;

import java.text.SimpleDateFormat;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.PrintWriter;

import java.security.MessageDigest;

import java.math.BigInteger;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.CreateMode;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import logger.LogSetup;
import app_kvServer.IKVServer.CacheStrategy;
import app_kvServer.IKVServer.Status;
import app_kvECS.ZooKeeperWatcher;

import ecs.ECSNode;
import ecs.IECSNode;
import ecs.IECSNode.NodeEvent;

import exceptions.InvalidMessageException;

// Code citation: https://stackoverflow.com/a/943963

public class ECS implements IECSClient {

    private static final CacheStrategy DEFAULT_CACHE_STRATEGY = CacheStrategy.LRU;
    private static final int DEFAULT_CACHE_SIZE = 16;

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ECS> ";
    private static boolean shutdown = false;
    private boolean running = false;
    private static TreeMap<String, ECSNode> active_servers = new TreeMap<String, ECSNode>();
    private Queue<String> available_servers = new LinkedList<>();
    private HashMap<String, String> movedServers = new HashMap<String, String>();
    private HashMap<String, String> crashedServers = new HashMap<String, String>();
    private int zkPort;
    private int port;
    private String ECSIP;
    private String rawMetadata = "";
    private ServerSocket serverSocket;

    public static ZooKeeper _zooKeeper = null;
    public static String _rootZnode = "/servers";

    public int testGetServerCount() {
        logger.info("getServerCount: " + active_servers.size());
        return active_servers.size();
    }

    public int testGetAvailServerCount() {
        logger.info("getServerCount: " + available_servers.size());
        return available_servers.size();
    }

    /**
     * Main entry point for the ECS application.
     * 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
            new LogSetup("logs/ecs_" + fmt.format(new Date()) + ".log", Level.INFO, true);
            if (args.length != 3) {
                logger.error("Error! Invalid number of arguments!");
                logger.error("Usage: ECS <ECS port> <ZooKeeper port> <config file>!");
                System.exit(1);
            } else {
                int port = Integer.parseInt(args[0]);
                int zkPort = Integer.parseInt(args[1]);
                String config = args[2];
                ECS app = new ECS(port, zkPort, config);
                final Thread mainThread = Thread.currentThread();
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    public void run() {
                        try {
                            terminate();
                        } catch (Exception e) {
                            logger.error("Error while completing the shutdown");
                            exceptionLogger(e);
                        }
                    }
                });
                app.run();
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            exceptionLogger(e);
            System.exit(1);
        }
    }

    public ECS(int port, int zkPort, String config) {
        try {
            this.port = port;
            this.zkPort = zkPort;
            this.ECSIP = getIP();
            if (this.ECSIP == null) {
                logger.fatal("Unable to obtain IP. Cannot continue");
                System.exit(1);
            }

            File myObj = new File(config);
            Scanner myReader = new Scanner(myObj);
            ArrayList<String> servers = new ArrayList<String>();
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                servers.add(data);
            }
            myReader.close();

            Collections.shuffle(servers);
            for (String server : servers) {
                available_servers.add(server);
            }
        } catch (Exception e) {
            exceptionLogger(e);
        }
    }

    public void testrun() {

        try {
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
            new LogSetup("logs/ecs_" + fmt.format(new Date()) + ".log", Level.INFO, true);
            initServer();
            logger.info("Initialized Server");
            initZooKeeper();
            logger.info("Initialized ZooKeeper");
        } catch (Exception e) {
            printError("Cannot start ZooKeeper!");
            logger.fatal("Cannot start ZooKeeper!");
            exceptionLogger(e);

            System.exit(1);
        }
        logger.info("Running ...");

        setRunning(true);
    }

    private void run() {
        try {
            initServer();
            logger.info("Initialized Server");
            initZooKeeper();
            logger.info("Initialized ZooKeeper");
        } catch (Exception e) {
            printError("Cannot start ZooKeeper!");
            logger.fatal("Cannot start ZooKeeper!");
            exceptionLogger(e);

            System.exit(1);
        }
        logger.info("Running ...");

        setRunning(true);
        while (isRunning()) {
            try {
                Socket client = serverSocket.accept();
                ClientConnection connection = new ClientConnection(client, this);
                new Thread(connection).start();

                logger.info("Connected to " + client.getInetAddress().getHostAddress() + ":"
                        + client.getPort());
            } catch (IOException e) {
                logger.error("Error! " + "Unable to establish connection. \n", e);
                exceptionLogger(e);
            }
        }
    }

    private String getIP() {
        String ip;
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();
                // filters out 127.0.0.1 and inactive interfaces
                if (iface.isLoopback() || !iface.isUp())
                    continue;

                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();

                    // *EDIT*
                    if (addr instanceof Inet6Address)
                        continue;

                    return addr.getHostAddress();
                }
            }
        } catch (Exception e) {
            logger.error("Error while obtaining server IP address");
            exceptionLogger(e);
        }

        return null;
    }

    private void initServer() {
        logger.info("Initializing server ...");

        try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " + serverSocket.getLocalPort());
        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            exceptionLogger(e);

            System.exit(1);
        }
    }

    private void initZooKeeper() throws Exception {
        logger.info("Initializing Zoo Keeper");
        _zooKeeper = new ZooKeeper("localhost:" + zkPort, 2000, new ZooKeeperWatcher(this));

        // Create the root node
        byte[] data = "".getBytes();
        _zooKeeper.create(_rootZnode, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        _zooKeeper.getChildren(_rootZnode,
                true);
    }

    @Override
    public boolean start() {
        try {
            for (Map.Entry<String, ECSNode> entry : active_servers.entrySet()) {
                String key = entry.getKey();
                ECSNode node = entry.getValue();

                // Only start the servers that are in the BOOT stage
                if (node.getStatus() != Status.BOOT) {
                    continue;
                }
                String path = String.format("%s/%s", _rootZnode, node.getNodeName());

                // Check if any move events have to take place, and issue them:
                String[] movedData = moveData(node.getNodeName(), false);
                if (movedData != null) {
                    String serverName = movedData[0].substring(movedData[0].lastIndexOf('/') + 1);
                    IECSNode movedServer = getNodeByKey(serverName);
                    if (movedServer != null && movedServer.getStatus() == Status.STARTED) {
                        logger.info("Have to move some data from: " + movedData[0] + " to " + node.getNodeName());
                        movedServers.put(movedData[0], node.getNodeName());
                        String data = NodeEvent.COPY.name() + "~"
                                + String.join(",", Arrays.copyOfRange(movedData, 1, movedData.length));
                        byte[] dataBytes = data.getBytes();
                        _zooKeeper.setData(movedData[0], dataBytes,
                                _zooKeeper.exists(movedData[0], false).getVersion());
                    } else {
                        logger.info("Not moving since " + serverName + " has not booted");
                    }
                } else {
                    logger.info("No move events for " + node.getNodeName());
                }

                logger.info("Going to start " + path);
                byte[] data = NodeEvent.START.name().getBytes();

                // Must subscribe here since its the initial entry point of the node
                _zooKeeper.setData(path, data, _zooKeeper.exists(path, true).getVersion());
            }

            if (movedServers.size() == 0) {
                try {
                    // Rapid fire of events causes missed events in ZooKeeper
                    Thread.sleep(500);
                } catch (Exception e) {
                    logger.error("Error while sleeping for node start");
                    exceptionLogger(e);
                }

                logger.info("No move events!");
                // Start the replication... Every server is a coordinator to 2 other replicas
                for (Map.Entry<String, ECSNode> entry : active_servers.entrySet()) {
                    ECSNode node = entry.getValue();
                    // Set all the BOOTED servers to STARTED
                    if (node.getStatus() == Status.BOOT) {
                        String key = entry.getKey();

                        node.setStatus(Status.STARTED);
                        active_servers.put(key, node);
                    }
                }

                updateMetadata();
                sendMetadata();
            } else {
                logger.info("Have to wait for " + movedServers.size() + " servers to finish moving data ...");
            }

            return true;
        } catch (

        Exception e) {
            logger.error("Error starting server!");
            exceptionLogger(e);

            return false;
        }
    }

    @Override
    public boolean stop() {
        logger.info("Broadcasting STOP event to all participating servers ...");
        byte[] data = NodeEvent.STOP.name().getBytes();

        boolean res = broadcastData(data);
        if (!res) {
            logger.error("Could not broadcast STOP event!");
        }

        return res;
    }

    @Override
    public boolean shutdown() {
        try {
            logger.info("Shutting Down ...");

            shutdown = true;
            completeShutdown();

            return true;
        } catch (Exception e) {
            logger.error(e);
            exceptionLogger(e);

            return false;
        }
    }

    public static void terminate() {
        try {
            logger.info("Terminating program ...");

            shutdown = true;
            completeShutdown();
        } catch (Exception e) {
            logger.error(e);
            exceptionLogger(e);
        }
    }

    @Override
    public synchronized IECSNode addNode(String cacheStrategy, int cacheSize) {
        logger.info("Attempting to add a node ...");
        try {
            if (available_servers.size() == 0) {
                logger.error("No more available servers!");
                return null;
            }
            String[] serverInfo = available_servers.remove().split("\\s+");

            String position = serverInfo[1] + ":" + serverInfo[2];
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(position.getBytes());
            byte[] digest = md.digest();

            BigInteger bi = new BigInteger(1, digest);
            String hash = String.format("%0" + (digest.length << 1) + "x", bi);
            // The lower bound will be NULL for now, but will fix it later once we're aware
            // of any other potential servers (cf. sendMetadata)
            String[] hashRange = { null, hash };
            ECSNode node = new ECSNode(serverInfo[0], serverInfo[1],
                    Integer.parseInt(serverInfo[2]), zkPort, hashRange, ECSIP);

            if (!node.initServer()) {
                logger.error("Could not SSH into server!");
            }

            logger.info("Added:" + serverInfo[0] + "(" + serverInfo[1] + ":" + serverInfo[2] + ")");
            active_servers.put(hash, node);

            try {
                // Rapid fire of events causes missed events in ZooKeeper
                Thread.sleep(500);
            } catch (Exception e) {
                logger.error("Error while sleeping for new nodes");
                exceptionLogger(e);
            }

            return node;
        } catch (Exception e) {
            logger.error(e);
            exceptionLogger(e);

            return null;
        }
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        logger.info("Attempting to add " + count + " nodes ...");

        ArrayList<IECSNode> nodes = new ArrayList<IECSNode>();
        for (int i = 0; i < count; ++i) {
            IECSNode newNode = addNode(cacheStrategy, cacheSize);

            if (newNode != null) {
                nodes.add(newNode);
            }
        }

        return nodes;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        for (String name : nodeNames) {
            boolean res = removeNode(name);
            if (!res) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String key) {
        try {
            for (IECSNode node : active_servers.values()) {
                if (node.getNodeName().equals(key)) {
                    return node;
                }
            }

            return null;
        } catch (Exception e) {
            logger.error(e);
            exceptionLogger(e);
            return null;
        }
    }

    private static boolean broadcastData(byte[] data) {
        // Broadcast to all servers
        try {
            for (IECSNode node : active_servers.values()) {
                String path = String.format("%s/%s", _rootZnode, node.getNodeName());
                if (_zooKeeper.exists(path, false) != null) {
                    _zooKeeper.setData(path, data, _zooKeeper.exists(path, false).getVersion());
                }
            }
            return true;
        } catch (Exception e) {
            logger.error("Error while shutting down");
            exceptionLogger(e);

            return false;
        }
    }

    private static void completeShutdown() {
        try {
            ArrayList<String> deleted_nodes = new ArrayList<String>();
            for (Map.Entry<String, ECSNode> entry : active_servers.entrySet()) {
                String key = entry.getKey();
                ECSNode node = entry.getValue();

                String path = String.format("%s/%s", _rootZnode, node.getNodeName());
                deleted_nodes.add(path);
                node.setStatus(Status.SHUTDOWN);
                active_servers.put(key, node);
            }

            for (String node : deleted_nodes) {
                _zooKeeper.delete(node, _zooKeeper.exists(node,
                        false).getVersion());
            }

            if (deleted_nodes.size() == 0) {
                logger.info("All servers shut down! Deleting root ZK node ...");
                // Delete the ZooKeeper root node
                _zooKeeper.delete(_rootZnode, _zooKeeper.exists(_rootZnode,
                        false).getVersion());
                logger.info("Root ZK node deleted. Shutdown complete");

                System.exit(0);
            }
        } catch (Exception e) {
            logger.error("Error while completing the shutdown");
            exceptionLogger(e);
        }
    }

    // Remove a random server (currently set as the first element)
    public boolean removeNode() {
        if (active_servers.size() == 0) {
            logger.error("No servers running to delete!");
            return false;
        }

        Map.Entry<String, ECSNode> entry = active_servers.entrySet().iterator().next();
        return removeNode(entry.getValue().getNodeName());
    }

    // Remove a specific server
    public boolean removeNode(String serverName) {
        logger.info("Removing " + serverName);
        String path = String.format("%s/%s", _rootZnode, serverName);
        Map.Entry<String, ECSNode> entry = getEntry(serverName);
        if (entry != null) {
            String key = entry.getKey();
            ECSNode node = entry.getValue();
            try {
                String shutdown = NodeEvent.SHUTDOWN.name();
                byte[] data = shutdown.getBytes();
                _zooKeeper.setData(path, data, _zooKeeper.exists(path, false).getVersion());

                node.setStatus(Status.SHUTDOWN);
                active_servers.put(key, node);

                return true;
            } catch (Exception e) {
                logger.error("Error while removing node!");
                exceptionLogger(e);
            }
        }

        logger.error("Could not find the node for removal");
        return false;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }
    
    public boolean isRunning() {
        return running;
    }

    public synchronized boolean nodeRemovedCreated() {
        ArrayList<Map.Entry<String, ECSNode>> nodesCrashed = new ArrayList<Map.Entry<String, ECSNode>>();
        boolean update_metadata = false;
        Iterator<Map.Entry<String, ECSNode>> active_iter = active_servers.entrySet().iterator();

        while (active_iter.hasNext()) {
            Map.Entry<String, ECSNode> entry = active_iter.next();

            String key = entry.getKey();
            ECSNode node = entry.getValue();
            String path = String.format("%s/%s", _rootZnode, node.getNodeName());
            try {
                // Node was (possibly) deleted/crashed
                if (_zooKeeper.exists(path, false) == null) {
                    if (node.getStatus() == Status.ADDED) {
                        continue;
                    }
                    boolean crash = false;
                    if (node.getStatus() == Status.SHUTDOWN) {
                        logger.info("Node gracefully closed: " + node.getNodeName());
                    } else {
                        logger.info("Node crashed: " + node.getNodeName());
                        crash = true;
                    }
                    active_iter.remove();

                    if (shutdown) {
                        List<String> nodes = _zooKeeper.getChildren(_rootZnode, false);
                        if (nodes.size() == 0) {
                            logger.info("All servers shut down! Deleting root ZK node ...");
                            // Delete the ZooKeeper root node
                            _zooKeeper.delete(_rootZnode, _zooKeeper.exists(_rootZnode,
                                    false).getVersion());
                            logger.info("Root ZK node deleted. Shutdown complete");

                            System.exit(0);
                        }
                    } else {
                        // Add it back to the list of available servers
                        String nodeConfig = String.format("%s %s %s", node.getNodeName(), node.getNodeHost(),
                                node.getNodePort());
                        logger.info("Re adding back: " + nodeConfig);
                        available_servers.add(nodeConfig);
                        if (crash) {
                            nodesCrashed.add(entry);
                        } else {
                            update_metadata = true;
                        }
                    }
                }
                // Node was (possibly) added
                else if (node.getStatus() == Status.ADDED) {
                    logger.info("Node addition of " + node.getNodeName());
                    node.setStatus(Status.BOOT);
                    active_servers.put(key, node);
                }
            } catch (Exception e) {
                logger.error("Error while checking for removed nodes");
                exceptionLogger(e);

                return false;
            }
        }

        for (Map.Entry<String, ECSNode> entry : nodesCrashed) {
            nodeCrashed(entry);
        }

        if (update_metadata) {
            // Broadcast the updated metadata
            updateMetadata();
            sendMetadata();
        }

        return true;
    }

    private void nodeCrashed(Map.Entry<String, ECSNode> entry) {
        String crashedEvent = NodeEvent.CRASH.name() + "~" + entry.getValue().getNodeName();
        byte[] data = crashedEvent.getBytes();
        logger.info("Sending:" + crashedEvent);

        try {
            Map.Entry<String, ECSNode> new_coord = active_servers.higherEntry(entry.getKey());
            // If it wraps around:
            if (new_coord == null) {
                new_coord = active_servers.firstEntry();
            }
            String path = String.format("%s/%s", _rootZnode, new_coord.getValue().getNodeName());
            _zooKeeper.setData(path, data, _zooKeeper.exists(path, false).getVersion());
            crashedServers.put(path, entry.getValue().getNodeName());
        } catch (Exception e) {
            logger.error("Error while sending CRASH to " + entry.getValue().getNodeName());
            exceptionLogger(e);
        }
    }

    public void crashComplete(String path) {
        logger.info("Adding another node due to server crash ...");
        // Start up any newly added servers due to the crash:
        addNode(DEFAULT_CACHE_STRATEGY.name(), DEFAULT_CACHE_SIZE);
        try {
            Thread.sleep(500);
        } catch (Exception e) {
            logger.error("Error while sleeping!");
            exceptionLogger(e);
        }
        logger.info("path:" + path);
        String crashedPath = crashedServers.remove(path);
        if (crashedPath == null) {
            logger.error("Unable to find deleted node!");
        }
        if (crashedServers.size() == 0) {
            logger.info("Starting up the servers ...");
            // As part of start, it also updates the metadata and broadcasts it
            start();
        } else {
            logger.info("Still waiting on " + crashedServers.size() + " crashed servers");
        }
    }

    private Map.Entry<String, ECSNode> getEntry(String serverName) {
        for (Map.Entry<String, ECSNode> entry : active_servers.entrySet()) {
            ECSNode node = entry.getValue();

            if (node.getNodeName().equals(serverName)) {
                return entry;
            }
        }

        return null;
    }

    private String[] moveData(String serverName, boolean delete) {
        Map.Entry<String, ECSNode> entry = getEntry(serverName);

        if (entry != null) {
            String key = entry.getKey();
            ECSNode node = entry.getValue();

            Map.Entry<String, ECSNode> successor = active_servers.higherEntry(key);
            // If it wraps around:
            if (successor == null) {
                successor = active_servers.firstEntry();
            }
            ECSNode successorVal = successor.getValue();
            if (successorVal.getNodeName().equals(serverName)) {
                return null;
            }
            String lower = active_servers.lowerKey(key);
            if (lower == null) {
                lower = active_servers.lastKey();
            }

            if (delete) {
                String znode = String.format("%s/%s", _rootZnode, serverName);
                return new String[] { znode, lower, successor.getKey(), successorVal.getNodeName() };
            } else {
                String znode = String.format("%s/%s", _rootZnode, successorVal.getNodeName());
                return new String[] { znode, lower, key, serverName };
            }
        }

        return null;
    }

    private void updateMetadata() {
        logger.info("Updating internal ECS metadata ...");
        // Update lower bounds now that all the participating servers have booted
        // Then, gather all server data
        List<String> serverData = new ArrayList();
        for (Map.Entry<String, ECSNode> entry : active_servers.entrySet()) {
            String key = entry.getKey();
            ECSNode node = entry.getValue();
            // Get the previous hash (or wrap around!)
            Map.Entry<String, ECSNode> prev = active_servers.lowerEntry(key);
            // Wrap around or if only participating server:
            if (prev == null) {
                prev = active_servers.lastEntry();
            }
            String[] hashRange = { prev.getKey(), key };
            node.setNodeHashRange(hashRange);
            // Update our internal info
            active_servers.put(key, node);
            serverData.add(node.getMeta());
        }
        rawMetadata = String.join(",", serverData);
        logger.info("Updated metadata: " + rawMetadata);
    }

    public void sendMetadata(String path) {
        logger.info("Sending metadata to " + path + " ...");

        String metadata = NodeEvent.METADATA.name() + "~" + rawMetadata;
        byte[] data = metadata.getBytes();
        logger.info("Sending:" + metadata);
        try {
            _zooKeeper.setData(path, data, _zooKeeper.exists(path, false).getVersion());
        } catch (Exception e) {
            logger.error("Error while sending metadata to " + path);
            exceptionLogger(e);
        }
    }

    public void sendMetadata() {
        logger.info("Broadcasting metadata to all participating servers ...");

        String metadata = NodeEvent.METADATA.name() + "~" + rawMetadata;
        byte[] data = metadata.getBytes();
        logger.info("Broadcasting:" + metadata);
        // Broadcast to all servers
        try {
            for (IECSNode node : active_servers.values()) {
                String path = String.format("%s/%s", _rootZnode, node.getNodeName());
                logger.info("\tTo " + path);
                _zooKeeper.setData(path, data, _zooKeeper.exists(path, false).getVersion());
            }
        } catch (Exception e) {
            logger.error("Error while broadcasting metadata");
            exceptionLogger(e);
        }
    }

    public synchronized void completeCopy(String path) {
        logger.info("Completing the move ...");
        String movedServer = movedServers.get(path);

        try {
            if (movedServer != null) {
                logger.info("Going to complete the move in: " + movedServer);
                Map.Entry<String, ECSNode> newServer = getEntry(movedServer);
                ECSNode node = newServer.getValue();
                node.setStatus(Status.STARTED);
                active_servers.put(newServer.getKey(), node);

                byte[] data = NodeEvent.MOVE.name().getBytes();
                _zooKeeper.setData(path, data, _zooKeeper.exists(path, false).getVersion());

                movedServers.remove(path);

                if (movedServers.size() == 0) {
                    // All moves have been issued. Now we can broadcast the UPDATED metadata
                    Thread.sleep(1000);
                    updateMetadata();
                    sendMetadata();
                }
            } else {
                logger.error("Unable to find the copy for: " + path);
            }
        } catch (Exception e) {
            logger.error("Error while completing boot for: " + path);
            exceptionLogger(e);
        }
    }

    private void startServer(String serverName) {
        try {
            String path = String.format("%s/%s", _rootZnode, serverName);
            byte[] data = NodeEvent.START.name().getBytes();
            _zooKeeper.setData(path, data, _zooKeeper.exists(path, false).getVersion());
        } catch (Exception e) {
            logger.error("Error while pending start server: " + serverName);
            exceptionLogger(e);
        }
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    public String listNodes() {
        ArrayList<String> nodeList = new ArrayList<String>();

        for (Map.Entry<String, ECSNode> entry : active_servers.entrySet()) {
            ECSNode node = entry.getValue();
            String serverName = node.getNodeName();
            if (node.getStatus() == Status.ADDED) {
                nodeList.add(serverName + " (Pending)");
            } else if (node.getStatus() == Status.BOOT) {
                nodeList.add(serverName + " (Booted)");
            } else {
                nodeList.add(serverName + " (Active)");
            }
        }

        for (String key : available_servers) {
            String serverName = key.split("\\s+")[0];
            nodeList.add(serverName + " (Inactive)");
        }

        return String.join(",", nodeList);
    }

    private static void exceptionLogger(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        logger.error(sw.toString());
    }
}
