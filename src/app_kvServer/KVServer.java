package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

import java.io.IOException;
import java.io.File;
import java.io.FileWriter;

import java.util.Date;
import java.util.Scanner;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;

import java.text.SimpleDateFormat;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;

import app_kvServer.ConcurrentNode;

import exceptions.InvalidMessageException;

public class KVServer implements IKVServer {

	private static final String STORAGE_DIRECTORY = "storage/";
	private static final CacheStrategy START_CACHE_STRATEGY = CacheStrategy.LRU;
	private static final int START_CACHE_SIZE = 16;
	private static final int MAX_READS = 100;
	public volatile boolean test = false;
	public volatile boolean wait = false;

	private static Logger logger = Logger.getRootLogger();
	private int port;
	private int cacheSize;
	private CacheStrategy strategy;
	private ServerSocket serverSocket;
	private boolean running;
	private File storageDirectory = new File(STORAGE_DIRECTORY);

	// TODO: I think the cache does indeed need to have concurrent access
	private LinkedHashMap<String, String> cache;
	// true = write in progress (locked) and false = data is accessible
	private ConcurrentHashMap<String, ConcurrentNode> clientRequests = 
		new ConcurrentHashMap<String, ConcurrentNode>();

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
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *                  to keep in-memory
	 * @param strategy  specifies the cache replacement strategy in case the cache
	 *                  is full and there is a GET- or PUT-request on a key that is
	 *                  currently not contained in the cache. Options are "FIFO",
	 *                  "LRU",
	 *                  and "LFU".
	 */
	public KVServer(int port, final int cacheSize, CacheStrategy strategy) {
		logger.info(
				"Creating server. Config: port=" + port + " Cache Size=" + cacheSize + " Caching strategy=" + strategy);

		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;

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

		// this.run();
	}

	@Override
	public int getPort() {
		return this.port;
	}

	@Override
	public String getHostname() {
		return serverSocket.getInetAddress().getHostName();
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
		return new File(STORAGE_DIRECTORY + key).isFile();
	}

	@Override
	public void clearStorage() {
		logger.info("Clearing storage");
		clearCache();
		File[] allContents = storageDirectory.listFiles();
		logger.info("Deleting " + allContents.length + " records");
		if (allContents != null) {
			for (File file : allContents) {
				file.delete();
			}
		}
		logger.info("Storage cleared");
	}

	@Override
	public KVMessage getKV(final int clientPort, final String key) throws InvalidMessageException {
		logger.info(clientPort+ "> GET for key=" + key);
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
			while (clientRequests.get(key).peek()[0] != clientPort);
			// while (clientRequests.get(key).peek()[1] != NodeOperation.READ.getVal());
			logger.info(clientPort + "> !!!!===Finished spinning===!!!!");

			clientRequests.get(key).acquire();
			readLocked = true;

			// Check if key exists
			if (!inStorage(key)) {
				getStat = StatusType.GET_ERROR;
				logger.info(clientPort + "> KEY does not exist");
			} else {
				// TODO: Key could exist here, but then gets deleted in another thread. HANDLE THIS
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
					File file = new File(STORAGE_DIRECTORY + key);
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
	public KVMessage putKV(final int clientPort, final String key, final String value) throws InvalidMessageException {
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
		// TODO: If a key gets deleted, I think clientRequests.get(key) would no longer work
		while (clientRequests.get(key).peek()[0] != clientPort);
		// while (clientRequests.get(key).peek()[0] != clientPort || 
		// 			clientRequests.get(key).availablePermits() != MAX_READS);

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
							if (cache != null){
								cache.remove(key);
							}

							File file = new File(STORAGE_DIRECTORY + key);
							file.delete();
							logger.info(clientPort + "> " + key + " successfully pruned");
						}
					};
					clientRequests.get(key).startPruning(pruneDelete);
				} else {
					logger.info(clientPort + "> Key does not exist - marked for deletion!");
					putStat = StatusType.DELETE_ERROR;
				}
			} catch(Exception e) {
				logger.info(clientPort + "> Deletion exception ...");
				logger.error(e);
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
				FileWriter myWriter = new FileWriter(STORAGE_DIRECTORY + key);
				myWriter.write(value);
				myWriter.close();
				// Sleep for a bit to let data take effect
				// Thread.sleep(1000);
			} catch (Exception e) {
				logger.error(e);
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
		setRunning(initializeServer());

		if (serverSocket != null) {
			while (isRunning()) {
				try {
					Socket client = serverSocket.accept();
					ClientConnection connection = new ClientConnection(client, this);
					new Thread(connection).start();

					logger.info("Connected to "
							+ client.getInetAddress().getHostAddress()
							+ ":" + client.getPort());
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
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
		setRunning(false);
		try {
			serverSocket.close();
			logger.info("Server closed");
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
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

	/**
	 * @param args contains the program's input args (here for signature purposes)
	 */
	public static void main(String[] args) {
		try {
			SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
			new LogSetup("logs/server_" + fmt.format(new Date()) + ".log", Level.ALL, true);
			if (args.length != 1) {
				logger.error("Error! Invalid number of arguments!");
				logger.error("Usage: Server <port>!");
				System.exit(1);
			} else {
				int port = Integer.parseInt(args[0]);
				// No need to use the run method here since the contructor is supposed to
				// start the server on its own
				// TODO: Allow passing additional arguments from the command line:
				new KVServer(port, START_CACHE_SIZE, START_CACHE_STRATEGY).run();
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
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
			return true;
		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if (e instanceof BindException) {
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	private void initializeStorage() {
		logger.info("Initializing storage ...");

		try {
			logger.info("Checking for storage directory at " + storageDirectory.getCanonicalPath());
			// Ensure storage directory exists
			if (!storageDirectory.exists()) {
				logger.info("Storage directory does not exist. Creating new directory.");
				storageDirectory.mkdir();
			}
		} catch (

		Exception e) {
			logger.error(e);
		}
	}

	private boolean isRunning() {
		return this.running;
	}

	public void setRunning(boolean run) {
		this.running = run;
	}
}
