package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.io.StringWriter;
import java.io.PrintWriter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.FileVisitResult;
import java.nio.file.attribute.BasicFileAttributes;

import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import java.util.Date;
import java.util.Scanner;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;

import java.text.SimpleDateFormat;

import java.security.MessageDigest;

import java.math.BigInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.CreateMode;

import logger.LogSetup;

import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;

import app_kvServer.ConcurrentNode;
import app_kvServer.ZooKeeperWatcher;

import ecs.IECSNode.NodeEvent;
import ecs.ECSNode;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;

import exceptions.InvalidMessageException;

public class KVServer implements IKVServer {

	private static final String ROOT_STORAGE_DIRECTORY = "storage";
	private static final CacheStrategy START_CACHE_STRATEGY = CacheStrategy.LRU;
	private static final int START_CACHE_SIZE = 16;
	private static final int MAX_READS = 100;
	// private static final int RECONCILIATION_INTERVAL = 30 * 1000;
	private static final int RECONCILIATION_INTERVAL = 5 * 1000;

	public volatile boolean test = false;
	public volatile boolean wait = false;

	private static Logger logger = Logger.getRootLogger();
	private int port;
	private int cacheSize;
	public String name;
	private String nameHash;
	private int zkPort;
	private String ECSIP;
	private CacheStrategy strategy;
	private ServerSocket serverSocket;
	private Status status = Status.BOOT;
	private String storageDirectory;
	private TreeMap<String, ECSNode> metadata = new TreeMap<String, ECSNode>();
	private String rawMetadata;
	private int metadataVersion = 0;
	private ArrayList<String> movedItems = new ArrayList<String>();
	private Semaphore reconciliationSem = new Semaphore(2);
	OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
	private String replica1 = null;
	private String replica2 = null;

	
	// remove after we can get avail servers from zookeeper
	private List<String> availableServers = Arrays.asList(new String[]{"server0;127.0.0.1;50019", "server1;127.0.0.1;50020"});
	private ArrayList<String> loadReplications = new ArrayList<String>();
	private boolean isLoadBalancer = false;

	// load replica servers should not do any load balance - this flag disables health check stuff
	private boolean isLoadReplica = false;
	private String parentName;

	public ZooKeeper _zooKeeper = null;
	public String _rootZnode = "/servers";

	// TODO: I think the cache does indeed need to have concurrent access
	private LinkedHashMap<String, String> cache;
	// true = write in progress (locked) and false = data is accessible
	private ConcurrentHashMap<String, ConcurrentNode> clientRequests = new ConcurrentHashMap<String, ConcurrentNode>();

	public ConcurrentHashMap<String, ConcurrentNode> getClientRequests() {
		return this.clientRequests;
	}

	public void setClientRequest(String key, ConcurrentNode clientRequest) {
		this.clientRequests.put(key, clientRequest);
	}

	/**
	 * Start KV Server at given port
	 * 
	 * @param port      given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed to
	 *                  keep in-memory
	 * @param strategy  specifies the cache replacement strategy in case the cache
	 *                  is full and there
	 *                  is a GET- or PUT-request on a key that is currently not
	 *                  contained in the cache.
	 *                  Options are "FIFO", "LRU", and "LFU".
	 * 
	 */
	public KVServer(final int cacheSize, CacheStrategy strategy, String name, int port,
			int zkPort, String ECSIP, boolean isLoadReplica, String parentName) {
		logger.info("Creating server. Config: port=" + port + " Cache Size=" + cacheSize
				+ " Caching strategy=" + strategy);

		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.name = name;
		this.zkPort = zkPort;
		this.ECSIP = ECSIP;
		this.isLoadReplica = isLoadReplica;
		this.parentName = parentName;
		if (isLoadReplica){
			this.storageDirectory = String.format("%s/%s/", ROOT_STORAGE_DIRECTORY, parentName);
		} else {
			this.storageDirectory = String.format("%s/%s/", ROOT_STORAGE_DIRECTORY, name);
		}

		if (strategy == CacheStrategy.LRU) {
			cache = new LinkedHashMap<String, String>(cacheSize, 0.75f, true) {
				protected boolean removeEldestEntry(Map.Entry eldest) {
					return size() > cacheSize;
				}
			};
		} else if (strategy == CacheStrategy.None) {
			logger.info("Not using cache");
		} else {
			logger.warn("Unimplemented caching strategy: " + strategy);
		}

		logger.info("Booting server ...");
		if (!initializeZooKeeper()) {
			logger.error("Could not not initialize ZooKeeper!");
			_zooKeeper = null;
		} else {
			logger.info("Spinning until server boots ...");
			// Keep spinning until signalled to start
			while (getStatus() == Status.BOOT)
				;

			logger.info("Stopped spinning.");
			if (getStatus() == Status.STARTED) {
				logger.info("Attempting to run server ...");
				run();
			}
			logger.info("Server closed");
		}
	}

	@Override
	public int getPort() {
		return this.port;
	}

	@Override
	public String getHostname() {
		return serverSocket.getInetAddress().getHostName();
	}

	public String getServerName() {
		return name;
	}

	public String getStorageDirectory() {
		return storageDirectory;
	}

	@Override
	public CacheStrategy getCacheStrategy() {
		return this.strategy;
	}

	@Override
	public int getCacheSize() {
		return this.cacheSize;
	}

	@Override
	public boolean inCache(String key) {
		if (cache != null) {
			return cache.containsKey(key);
		}
		return false;
	}

	@Override
	public void clearCache() {
		if (cache != null) {
			cache.clear();
		}
	}

	@Override
	public synchronized boolean inStorage(String key) {
		return new File(storageDirectory + key).isFile();
	}

	@Override
	public void clearStorage() {
		logger.info("Clearing storage ...");
		clearCache();

		deleteDirectory(storageDirectory);

		logger.info("Storage cleared.");
	}

	private void deleteDirectory(String deletedDirectory) {
		Path directory = Paths.get(deletedDirectory);
		try {
			Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					Files.delete(file);
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
					Files.delete(dir);
					return FileVisitResult.CONTINUE;
				}
			});
		} catch (Exception e) {
			logger.error("Error while deleting directory: " + deletedDirectory);
			exceptionLogger(e);
		}
	}

	@Override
	public KVMessage getKV(final int clientPort, final String key) throws InvalidMessageException {
		logger.info(clientPort + "> GET for key=" + key);
		KVMessage res;
		StatusType getStat = StatusType.GET_SUCCESS;
		String value = "";
		boolean queued = false;
		boolean readLocked = false;

		// TODO: Cleanup the clientRequests after the requests are completed
		clientRequests.putIfAbsent(key, new ConcurrentNode(MAX_READS));
		int[] node = { clientPort, NodeOperation.READ.getVal() };
		clientRequests.get(key).addToQueue(node);
		queued = true;

		if (test) {
			logger.debug(clientPort + "> !!!!===wait===!!!!");
			while (wait)
				;
			logger.info(clientPort + "> !!!!===DONE WAITING===!!!!");
		}

		try {
			while (clientRequests.get(key).peek()[0] != clientPort)
				;
			// while (clientRequests.get(key).peek()[1] != NodeOperation.READ.getVal());
			logger.info(clientPort + "> !!!!===Finished spinning===!!!!");

			clientRequests.get(key).acquire();
			readLocked = true;

			// Check if key exists
			if (!inStorage(key)) {
				getStat = StatusType.GET_ERROR;
				logger.info(clientPort + "> KEY does not exist");
			} else {
				// TODO: Key could exist here, but then gets deleted in another thread. HANDLE
				// THIS
				logger.info(clientPort + ">looks chill");
				logger.debug(clientPort + "> cnt:" + clientRequests.get(key).availablePermits());

				// See if marked for deletion:
				if (clientRequests.get(key).isDeleted()) {
					logger.info(clientPort + "> Key marked for deletion");
					if (queued) {
						logger.info(clientPort + "> !!!!===Removing from queue===!!!!");
						removeTopQueue(key);
						queued = false;
					}
					if (readLocked) {
						clientRequests.get(key).release();
						readLocked = false;
					}
					getStat = StatusType.GET_ERROR;
				} else if (inCache(key)) {
					logger.info(clientPort + "> Cache hit!");
					if (queued) {
						logger.info(clientPort + "> !!!!===Removing from queue===!!!!");
						removeTopQueue(key);
						queued = false;
					}
					if (readLocked) {
						clientRequests.get(key).release();
						readLocked = false;
					}
					value = cache.get(key);
				} else {
					File file = new File(storageDirectory + key);
					StringBuilder fileContents = new StringBuilder((int) file.length());
					logger.debug(clientPort + "> Gonna open the file now!");
					try (Scanner scanner = new Scanner(file)) {
						logger.debug(clientPort + "> Reading key file!");
						while (scanner.hasNextLine()) {
							fileContents.append(scanner.nextLine() + System.lineSeparator());
						}

						if (queued) {
							logger.info(clientPort + "> !!!!===Removing from queue===!!!!");
							removeTopQueue(key);
							queued = false;
						}
						if (readLocked) {
							clientRequests.get(key).release();
							readLocked = false;
						}

						value = fileContents.toString().trim();
						logger.debug(clientPort + "> Value=" + value);

						insertCache(key, value);
					} catch (Error e) {
						throw e;
					}
				}
			}
		} catch (Exception e) {
			logger.error(e);
			exceptionLogger(e);
			getStat = StatusType.GET_ERROR;
		} finally {
			if (queued) {
				logger.info(clientPort + "> !!!!===Removing from queue===!!!!");
				removeTopQueue(key);
			}
			if (readLocked) {
				clientRequests.get(key).release();
			}
		}

		res = new KVMessage(key, value, getStat);

		return res;
	}

	@Override
	public KVMessage putKV(final int clientPort, final String key, final String value)
			throws InvalidMessageException {
		logger.info(clientPort + "> PUT for key=" + key + " value=" + value);
		KVMessage res;
		StatusType putStat;

		// TODO: Cleanup the clientRequests after the requests are completed
		clientRequests.putIfAbsent(key, new ConcurrentNode(MAX_READS));
		NodeOperation op = value.equals("null") ? NodeOperation.DELETE : NodeOperation.WRITE;
		// add thread to back of list for this key - add is thread safe
		int[] node = { clientPort, op.getVal() };
		clientRequests.get(key).addToQueue(node);

		if (test) {
			logger.info(clientPort + "> !!!!===wait===!!!!");
			while (wait)
				;
			logger.info(clientPort + "> !!!!===DONE WAITING===!!!!");
		}

		// wait (spin) until threads turn and no one is reading
		// TODO: If a key gets deleted, I think clientRequests.get(key) would no longer
		// work
		while (clientRequests.get(key).peek()[0] != clientPort)
			;
		// while (clientRequests.get(key).peek()[0] != clientPort ||
		// clientRequests.get(key).availablePermits() != MAX_READS);

		logger.info(clientPort + "> !!!!===Finished spinning===!!!!");

		if (value.equals("null")) {
			// Delete the key
			logger.info(clientPort + "> Trying to delete record ...");
			putStat = StatusType.DELETE_SUCCESS;
			try {
				// TODO: Mark it as deleted then loop to see if anybody is reading/writing to it
				// If a write is after, keep the row by unmarking it as deleted (since the
				// operation cancels)
				// If a read is after, check the deleted flag, if its deleted, make sure to
				// return key DNE
				// Otherwise, return the key
				// If at any point the queue is empty, and the row is marked as deleted, THEN
				// remove it from the clientRequests
				// Delete it from the cache as well!
				// remove the top item from the list for this key
				if (!inStorage(key)) {
					logger.info(clientPort + "> Not in storage ...");
					putStat = StatusType.DELETE_ERROR;
				} else if (!clientRequests.get(key).isDeleted()) {
					logger.info(clientPort + "> Marked for deletion");
					clientRequests.get(key).setDeleted(true);

					Runnable pruneDelete = new Runnable() {
						@Override
						public void run() {
							logger.info(clientPort + "> Starting pruning thread");
							do {
								// logger.debug("Prune waiting");
							} while (!clientRequests.get(key).isEmpty());

							clientRequests.remove(key);
							if (cache != null) {
								cache.remove(key);
							}

							File file = new File(storageDirectory + key);
							file.delete();
							logger.info(clientPort + "> " + key + " successfully pruned");
						}
					};
					clientRequests.get(key).startPruning(pruneDelete);
				} else {
					logger.info(clientPort + "> Key does not exist - marked for deletion!");
					putStat = StatusType.DELETE_ERROR;
				}
			} catch (Exception e) {
				logger.info(clientPort + "> Deletion exception ...");
				logger.error(e);
				exceptionLogger(e);
				putStat = StatusType.DELETE_ERROR;
			}
		} else {
			// Insert/update the key
			logger.info(clientPort + "> Trying to insert/update record");
			try {
				if (!inStorage(key)) {
					// Inserting a new key
					logger.info(clientPort + "> Going to insert record");
					putStat = StatusType.PUT_SUCCESS;
				} else {
					// Updating a key
					logger.info(clientPort + "> Going to update record");
					putStat = StatusType.PUT_UPDATE;
					if (clientRequests.get(key).isDeleted()) {
						// Stop the deletion
						clientRequests.get(key).stopPruning();
						clientRequests.get(key).setDeleted(false);
					}
				}

				insertCache(key, value);
				FileWriter myWriter = new FileWriter(storageDirectory + key);
				myWriter.write(value);
				myWriter.close();
				// Sleep for a bit to let data take effect
				// Thread.sleep(1000);
			} catch (Exception e) {
				logger.error(e);
				exceptionLogger(e);
				putStat = StatusType.PUT_ERROR;
			}
		}

		// remove the top item from the list for this key
		removeTopQueue(key);
		logger.info(clientPort + "> !!!!===Removing from queue===!!!!");
		try {
			res = new KVMessage(key, value, putStat);
		} catch (InvalidMessageException ime) {
			throw ime;
		}

		return res;
	}

	@Override
	public void run() {
		if (_zooKeeper == null) {
			logger.error("ZooKeeper node not initialized");
			return;
		}
		if (serverSocket == null) {
			if (!initializeServer()) {
				logger.error("Coult not initialize server!");
				return;
			}
		}
		if (serverSocket != null) {
			logger.info("Server running ...");
			while (getStatus() != Status.STOPPED) {
				try {
					Socket client = serverSocket.accept();
					ClientConnection connection = new ClientConnection(client, this);
					new Thread(connection).start();

					logger.info("Connected to " + client.getInetAddress().getHostAddress() + ":"
							+ client.getPort());
				} catch (IOException e) {
					if (getStatus() != Status.STARTED)
						return;
					logger.error("Error! " + "Unable to establish connection. \n", e);
				}
			}
		}
		serverSocket = null;
		logger.info("Server stopped.");
	}

	// TODO: Difference between kill and close?
	@Override
	public void kill() {
		close();
	}

	@Override
	public void close() {
		logger.info("Closing server ...");
		try {
			if (serverSocket != null) {
				serverSocket.close();
				serverSocket = null;
			}
			setStatus(Status.STOPPED);
		} catch (IOException e) {
			logger.error("Error! " + "Unable to close socket on port: " + port, e);
		}
	}

	private void insertCache(String key, String value) {
		if (cache != null) {
			logger.info("Gonna put key in cache!");
			cache.put(key, value);
		}
	}

	public boolean inQueue(String key) {
		return clientRequests.containsKey(key);
	}

	public void crash(String server) {
		logger.info("Crash at:" + server);
		File destDir = new File(this.storageDirectory);
		String fromDest = String.format("%sreplica_%s/", this.storageDirectory, server);
		File fromDestDir = new File(fromDest);
		if (!destDir.exists()) {
			logger.info("No replica at " + fromDest + " ...");
		} else {
			logger.info("Copying files from replica at:" + fromDest);
			File[] directoryListing = fromDestDir.listFiles();
			for (File item : directoryListing) {
				try {
					Files.copy(item.toPath(),
							new File(this.storageDirectory + item.getName()).toPath(),
							StandardCopyOption.REPLACE_EXISTING);
				} catch (Exception e) {
					logger.error("Error while trying to move keys");
					exceptionLogger(e);
				}
			}
			logger.info("Copy completed.");
			String deletedDirectory = String.format("%s/%s", ROOT_STORAGE_DIRECTORY, server);
			logger.info("Deleting directory:" + deletedDirectory);
			deleteDirectory(deletedDirectory);
		}

		try {
			Thread.sleep(500);
		} catch (Exception e) {
			logger.error("Error while sleeping!");
			exceptionLogger(e);
		}
		setNodeData(NodeEvent.CRASH_COMPLETE.name());
	}

	/**
	 * @param args contains the program's input args (here for signature purposes)
	 */
	public static void main(String[] args) {
		try {
			if (args.length != 6) {
				logger.error("Error! Invalid number of arguments!");
				logger.error("Usage: Server <name> <port> <ZooKeeper Port> <ECS IP> <isLoadReplica> <parentName>!");
				System.exit(1);
			} else {
				String name = args[0];
				int port = Integer.parseInt(args[1]);
				int zkPort = Integer.parseInt(args[2]);
				String ECSIP = args[3];
				boolean isLoadReplica = Boolean.parseBoolean(args[4]);
				String parentName = args[5];
				
				SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
				new LogSetup("logs/" + name + "_" + fmt.format(new Date()) + ".log", Level.ALL, true);
				// No need to use the run method here since the contructor is supposed to
				// start the server on its own
				// TODO: Allow passing additional arguments from the command line:
				new KVServer(START_CACHE_SIZE, START_CACHE_STRATEGY, name, port, zkPort, ECSIP, isLoadReplica, parentName);
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
	}

	private synchronized void removeTopQueue(String key) {
		if (clientRequests.get(key).peek() != null) {
			clientRequests.get(key).poll();
		}
	}

	private boolean initializeServer() {
		logger.info("Initializing server ...");
		initializeStorage();

		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: " + serverSocket.getLocalPort());
			return true;
		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if (e instanceof BindException) {
				logger.error("Port " + port + " is already bound!");
			}

			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			logger.error(sw.toString());

			System.exit(1);
			return false;
		}
	}

	private boolean initializeZooKeeper() {
		logger.info("Creating & initializing ZooKeeper node ...");
		try {
			ZooKeeperWatcher zkWatcher = new ZooKeeperWatcher(this);
			_zooKeeper = new ZooKeeper(ECSIP + ":" + zkPort, 2000, zkWatcher);
			byte[] data = NodeEvent.BOOT.name().getBytes();

			String path = "";
			if(this.isLoadReplica) {
				path = String.format("%s/%s/%s", _rootZnode, parentName, name);
			} else {
				path = String.format("%s/%s", _rootZnode, name);
			}
			logger.info("Creating node:" + path);
			// We cannnot add children under ephemeral nodes! - need to create same type of node as root node
			_zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL);
			_zooKeeper.getData(path,
					zkWatcher, null);
			return true;
		} catch (Exception e) {
			logger.error("Error encountered while creating ZooKeeper node!");
			exceptionLogger(e);

			return false;
		}
	}

	private void initializeStorage() {
		logger.info("Initializing storage ...");

		try {
			File rootStorageDirectory = new File(ROOT_STORAGE_DIRECTORY);
			logger.info("Checking for root storage directory at " + rootStorageDirectory.getCanonicalPath());
			// Ensure storage directory exists
			if (!rootStorageDirectory.exists()) {
				logger.info("Root storage directory does not exist. Creating new directory ...");
				rootStorageDirectory.mkdir();
			}

			File storageDirectory = new File(String.format("%s/%s", ROOT_STORAGE_DIRECTORY, name));
			logger.info("Checking for server storage directory at " + storageDirectory.getCanonicalPath());
			// Ensure storage directory exists
			if (!storageDirectory.exists()) {
				logger.info("Server storage directory does not exist. Creating new directory ...");
				storageDirectory.mkdir();
			}
		} catch (Exception e) {
			logger.error(e);
			exceptionLogger(e);
		}
	}

	@Override
	public void initKVServer(String data) {
		logger.info("Loading metadata ...");
		rawMetadata = data;
		logger.info("Raw metadata:" + rawMetadata);
		String[] serverData = data.split(",");
		metadata.clear();
		for (String server : serverData) {
			String[] serverInfo = server.split(":");

			try {
				String[] hashRange = { serverInfo[3], serverInfo[4] };
				ECSNode node = new ECSNode(serverInfo[0], serverInfo[1],
						Integer.parseInt(serverInfo[2]), zkPort, hashRange, "RANDOM");
				logger.info("Server info:" + node.getMeta());

				// Remember this server's hash so we don't have to keep recalculating it
				if (serverInfo[0].equals(name)) {
					nameHash = serverInfo[4];
				}

				logger.info("Added:" + serverInfo[1] + ":" + serverInfo[2]);
				metadata.put(serverInfo[4], node);
			} catch (Exception e) {
				logger.error("Error while parsing metadata info, calculating hash of position");
				logger.error(e.getMessage());
				exceptionLogger(e);

				return;
			}
		}
	}

	public String getMetadataRaw() {
		return rawMetadata;
	}

	public TreeMap<String, ECSNode> getMetadata() {
		return metadata;
	}

	public int getMetadataVersion() {
		return metadataVersion;
	}

	@Override
	public void stop() {
		setStatus(Status.STOPPED);
	}

	@Override
	public void start() {
		if (getStatus() == Status.BOOT) {
			setStatus(Status.STARTED);
		} else if (getStatus() == Status.STOPPED) {
			setStatus(Status.STARTED);
			run();
		}
	}

	@Override
	public void lockWrite() {
		setStatus(Status.LOCKED);
	}

	@Override
	public void unLockWrite() {
		setStatus(Status.STARTED);
	}

	@Override
	public void shutDown() {
		try {
			logger.info("Shutting down server ...");

			logger.info("Shutting down ZooKeeper ...");
			_zooKeeper.close();
			logger.info("ZooKeeper shutdown.");

			clearStorage();

			close();
			System.exit(0);
		} catch (Exception e) {
			logger.error("Error while shutting down server");
			exceptionLogger(e);
		}
	}

	@Override
	public void update(String data) {
		logger.info("Updating metadata ...");
		metadataVersion += 1;
		initKVServer(data);
		updateReplicas();
		expireOldData();
		// TODO: Must temp stop the scheduledReconcilation
	}

	private void updateReplicas() {
		logger.info("Updating any necessary replicas ...");
		ArrayList<String> replica_updates = new ArrayList<String>();

		Map.Entry<String, ECSNode> after1 = metadata.higherEntry(nameHash);
		if (after1 == null) {
			after1 = metadata.firstEntry();
		}
		if (after1.getKey() != nameHash) {
			if (replica1 == null || !replica1.equals(after1.getValue().getNodeName())) {
				replica1 = after1.getValue().getNodeName();
				replica_updates.add(after1.getValue().getNodeHost() + ":" + after1.getValue().getNodePort() + ":"
						+ after1.getValue().getNodeName());
			}

			Map.Entry<String, ECSNode> after2 = metadata.higherEntry(after1.getKey());
			if (after2 == null) {
				after2 = metadata.firstEntry();
			}
			if (after2.getKey() != nameHash) {
				if (replica2 == null || !replica2.equals(after2.getValue().getNodeName())) {
					replica2 = after2.getValue().getNodeName();
					replica_updates.add(after2.getValue().getNodeHost() + ":" + after2.getValue().getNodePort() + ":"
							+ after2.getValue().getNodeName());
				}
			} else {
				replica2 = null;
			}
		} else {
			replica1 = null;
			replica2 = null;
		}

		if (replica_updates.size() > 0) {
			String replicas = String.join(",", replica_updates);
			logger.info("Due to metadata updates, must replicate data to: " + replicas);
			replicate(replicas, false);
		}
	}

	private void expireOldData() {
		logger.info("Expiring old data (no longer responsibile for)");

		File dir = new File(storageDirectory);
		if (!dir.exists()) {
			logger.info("Storage directory does not exist. Creating new directory ...");
			dir.mkdir();
			return;
		}
		File[] directoryListing = dir.listFiles();
		for (File item : directoryListing) {
			if (item.isDirectory()) {
				String coordinator = item.getName().split("_")[1];
				// Check if coordinator is still the coordinator (must be within 2 levels)
				Map.Entry<String, ECSNode> prev1 = metadata.lowerEntry(nameHash);
				// Wrap around
				if (prev1 == null) {
					prev1 = metadata.lastEntry();
				}
				Map.Entry<String, ECSNode> prev2 = metadata.lowerEntry(prev1.getKey());
				if (prev2 == null) {
					prev2 = metadata.lastEntry();
				}
				if (coordinator.equals(prev1.getValue().getNodeName())
						|| coordinator.equals(prev2.getValue().getNodeName())) {
					logger.info("Is a replica for " + coordinator);
					continue;
				}
				logger.info("No longer a replica of " + coordinator);
				deleteDirectory(storageDirectory + item.getName());
			}
		}
	}

	public synchronized void setNodeData(String data) {
		try {
			logger.info("Sending:" + data);
			byte[] dataBytes = data.getBytes();
			String path = String.format("%s/%s", _rootZnode, name);
			_zooKeeper.setData(path, dataBytes, _zooKeeper.exists(path, false).getVersion());
		} catch (Exception e) {
			logger.error("Error setting node data!");
			exceptionLogger(e);
		}
	}

	private boolean rangeFunction(String[] range, String hash) {
		// Have to compare if flipped as well (by virture of it being a circle ring)
		if (range[0].compareTo(range[1]) == 0)
			return true;

		return ((hash.compareTo(range[0]) >= 0
				&& hash.compareTo(range[1]) <= 0)
				// The following is if flipped, but less than 0xFFF (not wrapped around)...
				// of if flipped, but greater than 0xFFF (wrapped around). Hence XNOR
				|| (range[0].compareTo(range[1]) > 0
						&& !(hash.compareTo(range[1]) >= 0
								^ hash.compareTo(range[0]) > 0)));
	}

	public boolean inRange(String key) {
		// Get hash of the key
		String hash = "";
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(key.getBytes());
			byte[] digest = md.digest();

			BigInteger bi = new BigInteger(1, digest);
			hash = String.format("%0" + (digest.length << 1) + "x", bi);
		} catch (Exception e) {
			logger.error("Error while trying to see if key " + key + " is in range");
			exceptionLogger(e);

			return false;
		}

		ECSNode serverNode = metadata.get(nameHash);
		if (serverNode == null) {
			logger.error("Server data is missing!");
			return false;
		}
		// Ensure we have a valid hash range
		if (serverNode.getNodeHashRange()[0] == null || serverNode.getNodeHashRange()[1] == null) {
			logger.error("One/both range bounds are null. Cannot check if a key is in range!");
			return false;
		}
		// Now, check if hash is within this node's hash ranges

		return rangeFunction(serverNode.getNodeHashRange(), hash);
	}

	@Override
	public void moveData(String[] range, String destServer) {
		// Move the data to the destination server
		logger.info("Moving to " + destServer + ". And locking server ...");
		setStatus(Status.LOCKED);

		File dir = new File(this.storageDirectory);
		String dest = String.format("%s/%s/", ROOT_STORAGE_DIRECTORY, destServer);
		File destDir = new File(dest);
		if (!destDir.exists()) {
			logger.info("Destination directory does not exist. Creating new directory ...");
			destDir.mkdir();
		}
		File[] directoryListing = dir.listFiles();
		for (File item : directoryListing) {
			try {
				logger.info("File name:" + item.getName());
				if (item.isDirectory()) {
					logger.info("File is a directory. Continuing...");
					continue;
				}
				MessageDigest md = MessageDigest.getInstance("MD5");
				md.update(item.getName().getBytes());
				byte[] digest = md.digest();

				BigInteger bi = new BigInteger(1, digest);
				String hash = String.format("%0" + (digest.length << 1) + "x", bi);
				if (rangeFunction(range, hash)) {
					logger.info("Going to move:" + item.getName());
					logger.info("From: " + item.toPath());
					logger.info("To: " + new File(dest + item.getName()).toPath());
					movedItems.add(item.getName());

					Files.copy(item.toPath(),
							new File(dest + item.getName()).toPath(),
							StandardCopyOption.REPLACE_EXISTING);
				}
			} catch (Exception e) {
				logger.error("Error while trying to move keys");
				exceptionLogger(e);
			}
		}

		try {
			Thread.sleep(500);
		} catch (Exception e) {
			logger.error("Error while sleeping!");
			exceptionLogger(e);
		}
		setNodeData(NodeEvent.COPY_COMPLETE.name());
	}

	public synchronized void reconcileData(boolean pendingShutdown) {
		logger.info("Reconciling data ...");

		ArrayList<String> replicas = new ArrayList<String>();
		Map.Entry<String, ECSNode> after1 = metadata.higherEntry(nameHash);
		if (after1 == null) {
			after1 = metadata.firstEntry();
		}
		if (after1 != null && after1.getKey() != nameHash) {
			replicas.add(after1.getValue().getNodeHost() + ":" + after1.getValue().getNodePort() + ":"
					+ after1.getValue().getNodeName());

			Map.Entry<String, ECSNode> after2 = metadata.higherEntry(after1.getKey());
			if (after2 == null) {
				after2 = metadata.firstEntry();
			}

			if (after2 != null && after2.getKey() != nameHash) {
				replicas.add(after2.getValue().getNodeHost() + ":" + after2.getValue().getNodePort() + ":"
						+ after2.getValue().getNodeName());
			}
		}

		// Move the data to the two replica servers
		String res = String.join(",", replicas);
		logger.info("Reconciled data:" + res);
		replicate(res, pendingShutdown);
	}

	private void scheduleReconciliation() {
		logger.info(
				"Starting the scheduled reconciliation service (running every " + RECONCILIATION_INTERVAL / 1000
						+ " seconds) ...");
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

		scheduler.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					while (true) {
						// This is weird, I know (acquiring then releasing immediately, but it makes
						// sense lol)
						reconciliationSem.acquire();
						reconciliationSem.release();
						logger.info("Trying to see if other replications are in progress ...");
						int requiredPermits = 2;
						if (replica1 == null) {
							requiredPermits -= 1;
						}
						if (replica2 == null) {
							requiredPermits -= 1;
						}
						if (reconciliationSem.availablePermits() >= requiredPermits) {
							logger.info("No replications in progress. Proceeding ...");
							reconcileData(false);
							break;
						} else {
							logger.info(requiredPermits + " replication(s) in progress. Waiting ...");
						}
					}
				} catch (Exception e) {
					logger.error("Error while running the reconciliation process scheduled event");
					exceptionLogger(e);
				}
			}
		}, 0, RECONCILIATION_INTERVAL, TimeUnit.MILLISECONDS);
	}

	public void completeMove() {
		logger.info("Completing the move by deleting the items ...");

		for (String item : movedItems) {
			try {
				logger.info("Deleting " + item);
				File itemFile = new File(storageDirectory + item);
				itemFile.delete();
			} catch (Exception e) {
				logger.error("Error while trying to delete file");
				exceptionLogger(e);
			}
		}

		logger.info("Deletion completed");
		movedItems.clear();
		setStatus(Status.STARTED);
	}

	public synchronized Status getStatus() {
		return this.status;
	}

	private synchronized void setStatus(Status run) {
		this.status = run;
	}

	public void loadMetadata(String data) {
		if (rawMetadata == null || rawMetadata.isEmpty()) {
			initKVServer(data);
			updateReplicas();
			scheduleReconciliation();
			if(!isLoadReplica) scheduleSelfHealthCheck();
		} else {
			update(data);
		}
	}

	private void replicate(String destinations, final boolean pendingShutdown) {
		// TODO: When replicating, also be sure to check if any deleted items in the
		// coordinator are deleted in the replica
		String[] d = destinations.split(",");
		Thread[] threads = new Thread[d.length];
		for (int i = 0; i < d.length; ++i) {
			String destination = d[i];

			String[] serverInfo = destination.split(":");
			final String address = serverInfo[0];
			final int port = Integer.parseInt(serverInfo[1]);
			final String serverName = serverInfo[2];

			final File dir = new File(this.storageDirectory);
			final String dest = String.format("%s/%s/", ROOT_STORAGE_DIRECTORY,
					serverName);

			final boolean main_directory = pendingShutdown && i == 0;
			threads[i] = new Thread(new Runnable() {
				public void run() {
					try {
						reconciliationSem.acquire();
					} catch (Exception e) {
						logger.error("Error while trying to obtain the reconciliation semaphore");
						exceptionLogger(e);
					}

					logger.info("Replicating this server's data to " + address + ":" + port + " in new thread");
					// Create a replica directory
					String updatedDest = dest;
					if (!main_directory) {
						updatedDest = String.format("%sreplica_%s/", dest, name);
					}
					File destDir = new File(updatedDest);

					if (!destDir.exists()) {
						logger.info("Destination directory does not exist. Creating new directory ...");
						destDir.mkdirs();
					}

					// Get the already existing entries in the destination server
					Set<String> existingEntries = new HashSet<String>();
					File[] destListing = destDir.listFiles();
					for (File item : destListing) {
						existingEntries.add(item.getName());
					}

					File[] directoryListing = dir.listFiles();
					for (File item : directoryListing) {
						try {
							if (item.isDirectory()) {
								continue;
							}
							logger.info("Replicating: " + item.getName());
							logger.info("From: " + item.toPath());
							logger.info("To: " + new File(updatedDest + item.getName()).toPath());

							Files.copy(item.toPath(),
									new File(updatedDest + item.getName()).toPath(),
									StandardCopyOption.REPLACE_EXISTING);
							existingEntries.remove(item.getName());
						} catch (Exception e) {
							logger.error("Error while trying to replicate data");
							exceptionLogger(e);
						}
					}

					// Whatever is left in the set are the keys that should be deleted
					for (String key : existingEntries) {
						logger.info(key + " no longer exists. Deleting ...");
						File keyFile = new File(updatedDest + key);
						if (keyFile.delete()) {
							logger.info(key + " deleted.");
						} else {
							logger.error("Failed to deleted " + key);
						}
					}

					logger.info("Replication complete in " + serverName + "!");
					reconciliationSem.release();
				}
			});
			threads[i].start();
		}

		if (pendingShutdown) {
			logger.info("Waiting for replications to finish ...");
			try {
				for (int i = 0; i < d.length; ++i) {
					threads[i].join();
				}
			} catch (Exception e) {
				logger.error("Error while waiting for replication threads to complete!");
				exceptionLogger(e);
			}
		}
	}

	private static void exceptionLogger(Exception e) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		logger.error(sw.toString());
	}

	private void scheduleSelfHealthCheck() {
		logger.info(
				"Starting the scheduled autoscale service (running every " + HEALTHCHECK_INTERVAL / 1000
						+ " seconds) ...");
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
		
		scheduler.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					// What % CPU load this current JVM is taking, from 0.0-1.0
					// once theres multiple servers we need to collect data from all the servers - HOW?  
					// may need to use client connectsions 
					// amount of requests -> periodcially calculating throughput 
					// currently just using % CPU load this current JVM is taking, from 0.0-1.0
					double load = osBean.getProcessCpuLoad();	
					if(load > UPPER_THRESHOLD){
						autoScaleUp();
					} else if (load < LOWER_THRESHOLD ) { 
						autoScaleDown(); 
					}					
					logger.info( "Server " + serverName + " is healthy!")
				} catch (Exception e) {
					logger.error("Error while getting the relative load of the current server process!");
					exceptionLogger(e);
				}
			}
		}, 0, HEALTHCHECK_INTERVAL, TimeUnit.MILLISECONDS);
	}



	private List<String> getAvailServers(){
		// write code here to get available servers from zookeeper
		// for now using dummy code
		return availableServers;
	}

	private boolean removeFromAvailServers(List<String> replica){
		// write code here to remove servers from zookeeper
		// for now removing from list
		for(String server : availableServers){
			if(replica.contains(server)){
				availableServers.remove(server);
			}
		}
		return true;
	}

	private void autoScaleUp(int maxCount) {
		// First get list of avail servers
		List<String> avail = getAvailServers();

		// grab up to max count of servers and remove from list - using max count
		// because we dont always need to add 2, besides the first time adding replica
		List<String> replica;
		int cnt = 0;
		for(int i = 0; i < avail.size(); ++i){
			replica.add(avail.get(i));
			cnt++;
			if(cnt == maxCount) break;
		}

		// remove from avail servers in zookeeper and save replica list
		loadReplications.addAll(replica);
		removeFromAvailServers(replica);

		// Start each server and make sure they point to same storage and everything.
		// Copied code from how ECS spins up new servers
		boolean status = startLoadReplicas(replica);

		//Set loadBalance flag to true
		isLoadBalancer = true;

	}

	private void autoScaleDown() { 
		// if done with sub directories no need to make sure data is consistent
		// as it is always going to be in sink 
		int repCount = loadReplications.size();

		// do this for zookeeper obviously 
		if(repCount > 2){
			// remove one server -> or calculate how many to remove from system load
			// lowest threshold or if overall system usage gets lower.. .etc 
			
			removeFromAvailServers(loadReplications.remove(loadReplications.size() -1);
		} else { 
			// remove all 
			removeFromAvailServers(loadReplications);
			isLoadBalancer = false;
		}
	}

	private boolean startLoadReplicas(List<String> replica) {
		int err = 0;
		// from replica list - start server (copied from ECSNODE)
		for(String Item: replica){
			logger.info("Intializing server ... \nRunning script ...");
			String script = "script.sh";

			String[] serverInfo = Item.split(";");
			String childName = serverInfo[0];
			String host = serverInfo[1];
			String port = serverInfo[2];

			// Parent name is needed to make sure replica uses same file dir
			Runtime run = Runtime.getRuntime();
			String[] envp = { "host=" + host, "name=" + childName, "port=" + port,
					"zkPort=" + this.zkPort, "ECS_host=" +
							this.ECSIP, "isLoadReplica=" + true, "parentName=" + this.name
			};
			try {
				final Process proc = run.exec(script, envp);
				new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							logger.info("Attempting to SSH ...");
							proc.waitFor();
							int exitStatus = proc.exitValue();
							if (exitStatus != 0) {
								logger.error("Error in calling new server:" + exitStatus);
							}
						} catch (Exception e) {
							logger.error("Exception in calling new server!");
							e.printStackTrace();
						}
					}
				}).start();

				// Add code here to send metadata to child server using this.zookeper
				// That way server is properly started and accepting connections

			} catch (Exception e) {
				e.printStackTrace();
				err += 1;
			}
		}
		return err ? false : true;
	}

}

