package app_kvECS;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.net.Socket;

import java.util.Collection;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import ecs.ECSNode;
import ecs.IECSNode;

import shared.messages.KVMessage;
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
	ECS server;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * 
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, ECS server) {
		this.clientSocket = clientSocket;
		this.server = server;
		this.isOpen = true;
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
			while (isOpen) {
				try {
					Long start = System.nanoTime();
					KVMessage latestMsg = receiveMessage();
					if (latestMsg == null)
						return;
					switch (latestMsg.getStatus()) {
						case START:
							KVMessage startRes = start();
							sendMessage(startRes);
							sum += System.nanoTime() - start;
							break;
						case STOP:
							KVMessage stopRes = stop();
							sendMessage(stopRes);
							sum += System.nanoTime() - start;
							break;
						case SHUTDOWN:
							KVMessage shutdownRes = shutdown();
							sendMessage(shutdownRes);
							sum += System.nanoTime() - start;
							break;
						case ADD_NODES:
							KVMessage addNodesRes = addNodes(latestMsg.getValue());
							sendMessage(addNodesRes);
							sum += System.nanoTime() - start;
							break;
						case REMOVE_NODE:
							KVMessage removeNodeRes = removeNode(latestMsg.getValue());
							sendMessage(removeNodeRes);
							sum += System.nanoTime() - start;
							break;
						case LIST:
							KVMessage listRes = list();
							sendMessage(listRes);
							sum += System.nanoTime() - start;
							break;
						case SHUTDOWN_ECS:
							shutDownECS();
							break;
						case HEARTBEAT:
							// Just echo it back
							sendMessage(latestMsg);
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

	private KVMessage start() throws InvalidMessageException {
		logger.info("<"
				+ clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getPort() + "> (START)");

		byte[] msgBytes = { StatusType.ACK.getVal() };
		if (!server.start()) {
			msgBytes[0] = StatusType.NACK.getVal();
		}

		KVMessage msg = new KVMessage(msgBytes);
		return msg;
	}

	private KVMessage stop() throws InvalidMessageException {
		logger.info("<"
				+ clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getPort() + "> (STOP)");

		byte[] msgBytes = { StatusType.ACK.getVal() };
		if (!server.stop()) {
			msgBytes[0] = StatusType.NACK.getVal();
		}

		KVMessage msg = new KVMessage(msgBytes);
		return msg;
	}

	private KVMessage shutdown() throws InvalidMessageException {
		logger.info("<"
				+ clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getPort() + "> (SHUTDOWN)");

		byte[] msgBytes = { StatusType.ACK.getVal() };
		if (!server.shutdown()) {
			msgBytes[0] = StatusType.NACK.getVal();
		}

		KVMessage msg = new KVMessage(msgBytes);
		return msg;
	}

	private KVMessage addNodes(String value) throws InvalidMessageException {
		logger.info("<"
				+ clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getPort() + "> (ADD_NODES): " + value);

		KVMessage msg = null;
		String[] parsed = value.split(",");
		Collection<IECSNode> nodes = server.addNodes(Integer.parseInt(parsed[0]), parsed[1],
				Integer.parseInt(parsed[2]));

		if (nodes.contains(null) || nodes.size() != Integer.parseInt(parsed[0])) {
			byte[] msgBytes = { StatusType.NACK.getVal() };
			msg = new KVMessage(msgBytes);
		} else {
			ArrayList<String> nodeList = new ArrayList<String>();
			for (IECSNode node : nodes) {
				nodeList.add(node.getNodeName());
			}

			String res = String.join(",", nodeList);
			logger.info("Sending new node list:" + res);

			msg = new KVMessage("none", res, StatusType.ACK);
		}

		return msg;
	}

	private KVMessage removeNode(String value) throws InvalidMessageException {
		logger.info("<"
				+ clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getPort() + "> (REMOVE_NODE):" + value);

		byte[] msgBytes = { StatusType.ACK.getVal() };
		if (!server.removeNode(value)) {
			msgBytes[0] = StatusType.NACK.getVal();
		}

		KVMessage msg = new KVMessage(msgBytes);
		return msg;
	}

	private KVMessage list() throws InvalidMessageException {
		logger.info("<"
				+ clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getPort() + "> (LIST)");

		String serverList = server.listNodes();

		KVMessage msg = new KVMessage("none", serverList, StatusType.ACK);
		return msg;
	}

	private void shutDownECS() throws InvalidMessageException {
		logger.info("<"
				+ clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getPort() + "> (SHUTDOWN_ECS)");

		server.terminate();
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
}
