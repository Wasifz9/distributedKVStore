package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.net.SocketTimeoutException;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import shared.messages.KVMessage;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

import exceptions.InvalidMessageException;

public class KVStore implements KVCommInterface {

	private static final String PROMPT = "KVStore> ";
	private static final int RESPONSE_TIME = 90 * 1000;
	// private static final int RESPONSE_TIME = 10 * 1000;
	private static final int HEARTBEAT_INTERVAL = 1000;
	private static final int HEARTBEAT_TRANSMISSION = HEARTBEAT_INTERVAL * 10;
	private static final int HEARTBEAT_RETRIES = 3;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;
	
	public volatile static boolean test = false;
	private static Logger logger = Logger.getRootLogger();
	private boolean running;
	private String address;
	private int port;
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private long lastResponse;
	ScheduledFuture<?> heartbeatThread;
	private int missedHeartbeats = 0;
	public int output_port;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address the address of the KVServer
	 * @param port    the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
	}

	@Override
	public void connect() throws UnknownHostException, IOException {
		System.out.println(PROMPT + "Trying to connect ...");
		logger.info("Trying to connect ...");

		clientSocket = new Socket(address, port);
		clientSocket.setSoTimeout(RESPONSE_TIME);
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();
		output_port = clientSocket.getLocalPort();
		setRunning(true);
		setLastResponse(System.currentTimeMillis());

		// TODO: Try and figure out what the problem is:
		if (!test) {
			scheduleHeartbeat();
		}

		System.out.println(PROMPT + "Connected!");
		logger.info("Connection established to " + address + " on port " + port);
	}

	@Override
	public void disconnect() {
		try {
			System.out.println(PROMPT + "Trying to disconnect ...");
			logger.info("Trying to disconnect ...");
			setRunning(false);
			if (clientSocket != null) {
				input.close();
				output.close();
				clientSocket.close();
				clientSocket = null;
				System.out.println(PROMPT + "Connection closed!");
				logger.info("Connection closed!");
			}
		} catch (IOException ioe) {
			printError("Unable to close connection!");
			logger.error("Unable to close connection!", ioe);
		}
	}

	@Override
	public IKVMessage put(String key, String value) throws Exception {
		KVMessage msg = new KVMessage(key, value, StatusType.PUT);
		long sentTime = System.currentTimeMillis();
		long expectedTime = sentTime + RESPONSE_TIME;
		sendMessage(msg, false);

		KVMessage res = null;

		// while (true) {
		// 	res = receiveMessage(false);
		// 	if (System.currentTimeMillis() > expectedTime) break;
		// 	if (res != null && res.getStatus() != StatusType.HEARTBEAT) break;
		// }

		do {
			res = receiveMessage(false);
		} while (res.getStatus() == StatusType.HEARTBEAT && System.currentTimeMillis() < expectedTime);

		logger.debug("build new res put");
		if (res == null || res.getStatus() == StatusType.HEARTBEAT) {
			res = new KVMessage(key, "Timed out after " + RESPONSE_TIME / 1000 + " seconds", StatusType.PUT_ERROR);
		}

		return res;
	}

	@Override
	public IKVMessage get(String key) throws Exception {
		KVMessage msg = new KVMessage(key, null, StatusType.GET);
		long sentTime = System.currentTimeMillis();
		long expectedTime = sentTime + RESPONSE_TIME;
		sendMessage(msg, false);

		KVMessage res = null;

		// while (true) {
		// 	res = receiveMessage(false);
		// 	if (System.currentTimeMillis() > expectedTime) break;
		// 	if (res != null && res.getStatus() != StatusType.HEARTBEAT) break;
		// }

		do {
			res = receiveMessage(false);
		} while (res.getStatus() == StatusType.HEARTBEAT && System.currentTimeMillis() < expectedTime);

		if (res == null) {
			res = new KVMessage(key, "Timed out after " + RESPONSE_TIME / 1000 + " seconds", StatusType.GET_ERROR);
		}

		return res;
	}

	private void scheduleHeartbeat() {
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

		heartbeatThread = scheduler.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				if (getLastResponse() + HEARTBEAT_TRANSMISSION * (1 + missedHeartbeats) < System.currentTimeMillis()) {
					byte msgBytes[] = { StatusType.HEARTBEAT.getVal() };
					KVMessage msg;
					KVMessage res;
					
					try {
						msg = new KVMessage(msgBytes);
						sendMessage(msg, true);
						res = receiveMessage(true);
						setMissedHeartbeats(0);
					} catch (InvalidMessageException e) {
						logger.error("Unable to construct message!");
						logger.error(e);
					} catch (Exception e) {
						setMissedHeartbeats(missedHeartbeats + 1);
						if (missedHeartbeats > HEARTBEAT_RETRIES) {
							disconnect("Server unresponsive... Shutting down!");
						} else {
							if (missedHeartbeats == 1) {
								System.out.println();
								System.out.println(PROMPT + "Server not responding");
								logger.warn("Server not responding. Sending heartbeats ...");
							}
							logger.warn("Heartbeat " + missedHeartbeats + " of " + HEARTBEAT_RETRIES);
							System.out.println("Trial " + missedHeartbeats + " of " + HEARTBEAT_RETRIES);
						}
					}
				}
			}
		}, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
	}

	public int getMissedHeartbeats() {
		return missedHeartbeats;
	}

	public void setMissedHeartbeats(int missedHeartbeats) {
		this.missedHeartbeats = missedHeartbeats;
	}

	public void disconnect(String msg) {
		try {
			heartbeatThread.cancel(false);
			System.out.println(PROMPT + msg);
			System.out.print("KVClient> ");
			logger.warn(msg);
			setRunning(false);
			if (clientSocket != null) {
				input.close();
				output.close();
				clientSocket.close();
				clientSocket = null;
			}
		} catch (IOException ioe) {
			printError("Unable to close connection!");
			logger.error("Unable to close connection!", ioe);
		}
	}

	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean run) {
		running = run;
	}

	public long getLastResponse() {
		return lastResponse;
	}

	public synchronized void setLastResponse(long response) {
		lastResponse = response;
	}

	public synchronized void closeConnection() {
		disconnect("Client closed connection");
	}

	public synchronized KVMessage receiveMessage(boolean heartbeat) throws IOException, InvalidMessageException, Exception {
		if (!isRunning()) throw new IOException("Store is not running!");

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;

		logger.debug("First read:" + read);

		if (read == -1) {
			throw new Exception("Reached end of stream!");
		}

		setLastResponse(System.currentTimeMillis());

		while (read != LINE_FEED && read != -1 && reading) {/* LF, error, drop */
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				if (msgBytes == null) {
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			}

			/* only read valid characters, i.e. letters and numbers */
			// if((read > 31 && read < 127)) {
			bufferBytes[index] = read;
			index++;
			// }

			/* stop reading if DROP_SIZE is reached */
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}

			/* read next char from stream */
			read = (byte) input.read();
		}

		logger.debug("Last read:" + read);

		if (msgBytes == null) {
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}

		
		msgBytes = tmp;
		
		/* build final result */
		KVMessage msg = new KVMessage(msgBytes);
		
		if (!heartbeat) {
			logger.info("RECEIVE: STATUS="
			+ msg.getStatus() + " KEY=" + msg.getKey() + " VALUE=" + msg.getValue());
		}
		
		return msg;
	}

	public synchronized void sendMessage(KVMessage msg, boolean heartbeat) throws IOException {
		if (!isRunning()) throw new IOException("Store is not running!");

		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();

		if (!heartbeat) {
			logger.info("SEND: STATUS="
					+ msg.getStatus() + " KEY=" + msg.getKey() + " VALUE=" + msg.getValue());
		}
	}

	private void printError(String error) {
		System.out.println(PROMPT + "Error! " + error);
	}
}
