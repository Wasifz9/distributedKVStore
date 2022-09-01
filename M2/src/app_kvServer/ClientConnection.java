package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.io.File;

import java.util.Scanner;
import java.util.Map;

import java.net.Socket;

import java.security.MessageDigest;

import java.math.BigInteger;

import org.apache.log4j.Logger;

import app_kvServer.IKVServer.Status;

import ecs.ECSNode;

import shared.messages.KVMessage;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

import exceptions.InvalidMessageException;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	KVServer server;
	private int metadataVersion;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * 
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, KVServer server) {
		this.clientSocket = clientSocket;
		this.server = server;
		this.metadataVersion = server.getMetadataVersion();
		this.isOpen = true;
	}

	// A request is valid only if:
	// 1. The server is OPEN for requests
	// 2. The server is responsible for the requested key
	// 3. A write request and server is in the middle of reallocating data

	// TODO: Point #3 does not make sense since a GET OP should also not work if the
	// data is moved (?). Or should we just respond with a 404?
	private KVMessage validRequest(String key, boolean PUT_OP) {
		KVMessage res = null;

		try {
			if (this.server.getStatus() == Status.STOPPED) {
				res = new KVMessage(key, "Server is not running!", StatusType.SERVER_STOPPED);
			} else if (PUT_OP && this.server.getStatus() == Status.LOCKED) {
				res = new KVMessage(key,
						"Server is not is currently blocked for write requests due to reallocation of data!",
						StatusType.SERVER_WRITE_LOCK);
			} else {
				String responsible_server = getResponsibleServer(key);
				if (!responsible_server.equals(server.getServerName())) {
					if (PUT_OP) {
						logger.info("Incorrect server for PUT");
						res = new KVMessage(key, "", StatusType.SERVER_NOT_RESPONSIBLE);
					} else {
						logger.info("Not connected to coordinator. Trying to see if this server is a replica");
						res = tryGetKey(responsible_server, key);
					}
				}
			}
		} catch (Exception e) {
			logger.error("Error while composing message");
			logger.error(e.getMessage());
		}

		return res;
	}

	private String getResponsibleServer(String key) {
		// Get hash of the key
		String hash = "";
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(key.getBytes());
			byte[] digest = md.digest();

			BigInteger bi = new BigInteger(1, digest);
			hash = String.format("%0" + (digest.length << 1) + "x", bi);
		} catch (Exception e) {
			logger.error("Error while trying to get responsible server for key " + key);
			exceptionLogger(e);

			return null;
		}

		Map.Entry<String, ECSNode> successor = server.getMetadata().higherEntry(hash);
		// If it wraps around:
		if (successor == null) {
			successor = server.getMetadata().firstEntry();
		}
		return successor.getValue().getNodeName();
	}

	private KVMessage tryGetKey(String serverName, String key) {
		File dir = new File(server.getStorageDirectory());
		File[] directoryListing = dir.listFiles();
		String metadata = this.server.getMetadataRaw();
		KVMessage res = null;
		boolean isReplica = false;

		for (File possibleDir : directoryListing) {
			try {
				String replica_name = possibleDir.getName().substring(possibleDir.getName().lastIndexOf('_') + 1);
				if (possibleDir.isDirectory() && serverName.equals(replica_name)) {
					isReplica = true;
					File item = new File(possibleDir + "/" + key);
					if (item.isFile()) {
						// Read the file
						logger.info("Reading data at " + item.getCanonicalPath());
						StringBuilder fileContents = new StringBuilder((int) item.length());
						try (Scanner scanner = new Scanner(item)) {
							while (scanner.hasNextLine()) {
								fileContents.append(scanner.nextLine() + System.lineSeparator());
							}

							String value = fileContents.toString().trim();
							logger.info("Value at replica: " + value);
							res = new KVMessage(key, value, StatusType.GET_SUCCESS);
						} catch (Exception e) {
							logger.error("Error while reading the file at" + item.getCanonicalPath());
							exceptionLogger(e);
						}
					} else {
						res = new KVMessage(key, "", StatusType.GET_ERROR);
					}
				}
			} catch (Exception e) {
				logger.error("Error while trying to get keys from replicas");
				exceptionLogger(e);
			}
		}

		if (!isReplica) {
			logger.info(
					server.getServerName() + " is not a replica for the coordinator responsibile for key  "
							+ key);
			try {
				res = new KVMessage(key, metadata, StatusType.SERVER_NOT_RESPONSIBLE);
			} catch (Exception e) {
				exceptionLogger(e);
			}
		}

		return res;
	}

	/**
	 * Initializes and starts the client connection.
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		Long sum = (long) 0;

		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			sendMetadata();
			while (isOpen) {
				try {
					Long start = System.nanoTime();
					KVMessage latestMsg = receiveMessage();
					if (latestMsg == null)
						return;
					switch (latestMsg.getStatus()) {
						case PUT:
							KVMessage putRes = validRequest(latestMsg.getKey(), true);
							if (putRes == null) {
								putRes = putKV(latestMsg.getKey(), latestMsg.getValue());
							}
							sendMessage(putRes);
							sum += System.nanoTime() - start;
							break;
						case GET:
							KVMessage getRes = validRequest(latestMsg.getKey(), false);
							if (getRes == null) {
								getRes = getKV(latestMsg.getKey());
							}
							sendMessage(getRes);
							sum += System.nanoTime() - start;
							break;
						case HEARTBEAT:
							// Check to see if metadata has to be sent as well
							int serverMetadataVerion = server.getMetadataVersion();
							if (serverMetadataVerion != metadataVersion) {
								metadataVersion = serverMetadataVerion;
								logger.info("Metadata updated on server");
								logger.info("Piggybacking metadata on heartbeat ...");
								sendMetadata();
							} else {
								sendMessage(latestMsg);
							}
							break;
						default:
							logger.warn("<"
									+ clientSocket.getInetAddress().getHostAddress() + ":"
									+ clientSocket.getPort() + "> Unrecognized Status! Status="
									+ latestMsg.getStatus());

							// Send a bad request back to the client
							KVMessage errorRes = new KVMessage(latestMsg.getKey(),
									"Bad request! Unknown status",
									StatusType.BAD_REQUEST);
							sum += System.nanoTime() - start;
							if (errorRes != null)
								sendMessage(errorRes);
							break;
					}
					/*
					 * connection either terminated by the client or lost due to
					 * network problems
					 */
				} catch (IOException ioe) {
					logger.error("<"
							+ clientSocket.getInetAddress().getHostAddress() + ":"
							+ clientSocket.getPort() + "> Error! Connection lost!");
					isOpen = false;
				} catch (InvalidMessageException ime) {
					logger.error(ime);
				}
			}
		} catch (IOException ioe) {
			logger.error("<"
					+ clientSocket.getInetAddress().getHostAddress() + ":"
					+ clientSocket.getPort() + "> Error! Connection could not be established!", ioe);
		} finally {
			try {
				if (clientSocket != null) {
					logger.info("Thread " + "" + (int) Thread.currentThread().getId() + " Lat: " + sum);
					logger.info("<"
							+ clientSocket.getInetAddress().getHostAddress() + ":"
							+ clientSocket.getPort() + "> Tearing down connection ...");
					input.close();
					output.close();
					clientSocket.close();
					logger.info("<"
							+ clientSocket.getInetAddress().getHostAddress() + ":"
							+ clientSocket.getPort() + "> Connection closed");
				}
			} catch (IOException ioe) {
				logger.error("<"
						+ clientSocket.getInetAddress().getHostAddress() + ":"
						+ clientSocket.getPort() + "> Error! Unable to tear down connection!", ioe);
			}
		}
	}

	private void sendMetadata() {
		try {
			logger.info("Sending initial metadata ...");
			// Must send the initial metadata
			KVMessage metadataMessage = new KVMessage("0",
					this.server.getMetadataRaw(),
					StatusType.METADATA);
			sendMessage(metadataMessage);
			logger.info("Sent metadata");
		} catch (Exception e) {
			logger.error("Error while sending initial metadata!");
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			logger.error(sw.toString());
		}
	}

	private KVMessage putKV(String key, String value) throws InvalidMessageException {
		logger.info("<"
				+ clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getPort() + "> (PUT): KEY=" + key + " VALUE=" + value);

		return server.putKV(clientSocket.getPort(), key, value);
	}

	private KVMessage getKV(String key) throws InvalidMessageException {
		logger.info("<"
				+ clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getPort() + "> (GET): KEY=" + key);

		return server.getKV(clientSocket.getPort(), key);
	}

	/**
	 * Method sends a TextMessage using this socket.
	 * 
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream
	 */
	public void sendMessage(KVMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();

		if (msg.getStatus() != StatusType.HEARTBEAT) {
			logger.info("<"
					+ clientSocket.getInetAddress().getHostAddress() + ":"
					+ clientSocket.getPort() + "> (SEND): STATUS="
					+ msg.getStatus() + " KEY=" + msg.getKey() + " VALUE=" + msg.getValue());
		}
	}

	private KVMessage receiveMessage() throws InvalidMessageException, IOException {
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;

		if (read == -1) {
			return null;
		}

		// logger.debug("First read:" + read);

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

			/* stop reading is DROP_SIZE is reached */
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}

			/* read next char from stream */
			read = (byte) input.read();
		}

		// logger.debug("Last read:" + read);

		if (msgBytes == null) {
			logger.debug("null message");
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
			logger.debug("done null message");
		} else {
			logger.debug("not null message");
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
			logger.debug("done not null message");
		}

		msgBytes = tmp;
		logger.debug("make into kv message");
		/* build final result */
		KVMessage msg = null;

		try {
			msg = new KVMessage(msgBytes);
		} catch (InvalidMessageException e) {
			logger.error("Unable to construct message!");
			logger.error(e);
			throw (e);
		}

		if (msg != null && msg.getStatus() != StatusType.HEARTBEAT) {
			logger.info("<"
					+ clientSocket.getInetAddress().getHostAddress() + ":"
					+ clientSocket.getPort() + "> (RECEIVE): STATUS="
					+ msg.getStatus() + " KEY=" + msg.getKey() + " VALUE=" + msg.getValue());
		}

		return msg;
	}

	private static void exceptionLogger(Exception e) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		logger.error(sw.toString());
	}
}
