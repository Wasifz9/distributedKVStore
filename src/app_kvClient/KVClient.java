package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.net.UnknownHostException;
import java.net.SocketTimeoutException;

import java.util.Date;

import java.text.SimpleDateFormat;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import client.KVStore;
import client.KVCommInterface;

import shared.messages.IKVMessage;

public class KVClient implements IKVClient {

	private static final String PROMPT = "KVClient> ";
	
	private static Logger logger = Logger.getRootLogger();
	private BufferedReader stdin;
	private KVStore store;
    private boolean running = true;
	private int serverPort;

    @Override
    public void newConnection(String hostname, int port) throws UnknownHostException, IOException {
		logger.info("Trying to connect to: " + hostname + ":" + port);
		store = new KVStore(hostname, port);
		store.connect();
		logger.info("Connected");
    }

    @Override
    public KVCommInterface getStore() {
		return store;
    }

	/**
     * Main entry point for the KVClient application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
			SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
			new LogSetup("logs/client_" + fmt.format(new Date()) + ".log", Level.ALL, false);
			KVClient app = new KVClient();
			app.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }

    public void run() {
		// TODO: for debugging (delete later):
		// try {
		// 	if (store == null) {
		// 		this.newConnection("127.0.0.1", 9696);
		// 	}
		// } catch (Exception e) {
		// }
		
		while (running) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				handleCommand(cmdLine);
			} catch (IOException e) {
				setRunning(false);
				printError("CLI does not respond - Application terminated");
				logger.fatal("CLI does not respond - Application terminated");
			}
		}
	}

	private void setRunning(boolean running) {
		this.running = running;
	}

    private void handleCommand(String cmdLine) {
		if (cmdLine.trim().length() == 0) {
			return;
		}

		logger.info("User input: " + cmdLine);
		String[] tokens = cmdLine.split("\\s+");

		if (tokens[0].equals("connect")) {	
			if (tokens.length == 3) {
				connectCommand(tokens[1], tokens[2]);
			} else {
				printError("Expected 2 arguments: connect <address> <port>");
			}
		} else if (tokens[0].equals("disconnect")) {
			if (tokens.length == 1) {
				disconnectCommand();
			} else {
				printError("Expected 0 arguments");
			}
		} else if (tokens[0].equals("put")) {
			if (tokens.length >= 3) {
				if (store != null && store.isRunning()) {
					StringBuilder msg = new StringBuilder();
					for (int i = 2; i < tokens.length; i++) {
						msg.append(tokens[i]);
						if (i != tokens.length - 1) {
							msg.append(" ");
						}
					}

					putCommand(tokens[1], msg.toString());
				} else {
					printError("Not connected!");
				}
			} else {
				printError("Expected 2 arguments: put <key> <value>");
			}
		} else if (tokens[0].equals("get")) {
			if(tokens.length == 2) {
				if (store != null && store.isRunning()){
					getCommand(tokens[1]);
				} else {
					printError("Not connected!");
				}
			} else {
				printError("Expected 1 argument: get <key>");
			}
		} else if (tokens[0].equals("logLevel")) {
			if (tokens.length == 2) {
				Level level = setLevel(tokens[1]);
				if (level == null) {
					printError("No valid log level! Log Level unchanged");
					printPossibleLogLevels();
				} else {
					logger.setLevel(level);
					logger.info(PROMPT + 
						"Log level changed to level " + level);
				}
			} else {
				printError("Expected 1 argument: logLevel <level>");
			}
		} else if (tokens[0].equals("help")) {
			if (tokens.length == 1) {
				printHelp();
			} else {
				printError("Expected 0 arguments");
			}
		} else if (tokens[0].equals("quit")) {
			if (tokens.length == 1) {
				quitCommand();
				logger.info(PROMPT + "Application exit!");
			} else {
				printError("Expected 0 arguments");
			}
		} else {
			printError(tokens.toString());
			printError("Unknown command");
			printHelp();
		}
	}

	private void connectCommand(String hostname, String port) {
		try {
			newConnection(hostname, Integer.parseInt(port));
		} catch (NumberFormatException nfe) {
			printError("No valid address. Port must be a number!");
			logger.warn("Unable to parse argument <port>", nfe);
		} catch (UnknownHostException e) {
			printError("Unknown Host!");
			logger.warn("Unknown Host!", e);
		} catch (IOException e) {
			printError("Could not establish connection!");
			logger.warn("Could not establish connection!", e);
		}
	}

	private void putCommand(String key, String value) {
		logger.info("Trying to PUT key=" + key + " value=" + value);

		try {
			IKVMessage res = store.put(key, value);
			switch (res.getStatus()) {
				case PUT_SUCCESS:
					System.out.println("SUCCESS: " + res.getKey() + " inserted");
					break;
				case PUT_UPDATE:
					String msg = value.equals("null") ? " deleted" : " updated";
					System.out.println("SUCCESS: " + res.getKey() + msg);
					break;
				case PUT_ERROR:
					printError("PUT operation with key " + res.getKey() + " failed: " + res.getValue());
					break;
				case DELETE_SUCCESS:
					System.out.println("SUCCESS: DELETE operation with key " + res.getKey());
					break;
				case DELETE_ERROR:
					printError("DELETE operation with key " + res.getKey() + " failed: " + res.getValue());
					break;
				default:
					printError("Unknown PUT status!");
					logger.error("ERROR: Unknown status return for PUT:" + res.getStatus());
					break;
				}
		} catch (Exception e) {
			printError("Server Error!");
			logger.error(e);
		}
	}

	private void getCommand(String key) {
		logger.info("Trying to GET key=" + key);
		
		try {
			IKVMessage res = store.get(key);
			switch (res.getStatus()) {
				case GET_SUCCESS:
					System.out.println("SUCCESS: " + key + " = " + res.getValue());
					break;
				case GET_ERROR:
					printError("GET operation with key " + key + " failed: " + res.getValue());
					break;
				default:
					printError("Unknown GET response!");
					logger.error("ERROR: Unknown status return for GET:" + res.getStatus());
					break;
			}
		} catch (Exception e) {
			printError("Server Error!");
			logger.error(e);
		}
	}

    private void disconnectCommand() {
		if (store != null) {
			store.closeConnection();
			store = null;
			System.out.println("Disconnected");
			logger.info("Disconnected");
		} else {
			printError("Not connected!");
		}
	}

	private void quitCommand() {
		if (store != null) disconnectCommand();
		setRunning(false);
	}

    private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("KV CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t\t establishes a connection to the storage server based on the given server address and the port number of the storage service\n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t disconnects from the server\n");
		sb.append(PROMPT).append("put <key> <value>");
		sb.append("\t\t inserts a key-value pair into the storage server data structures, updates (overwrites) the current value with the given value if the server already contains the specified key or deletes the entry for the given key if <value> equals null\n");
		sb.append(PROMPT).append("get <key>");
		sb.append("\t\t retrieves the value for the given key from the storage server\n");
		
		
		sb.append(PROMPT).append("logLevel <level>");
		sb.append("\t\t changes the logLevel. Must be one of:\n");
		sb.append(PROMPT).append("\t\t\t");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		
		sb.append(PROMPT).append("help");
		sb.append("\t\t shows this help menu\n");
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t exits the program");
		
		System.out.println(sb.toString());
	}
	
	private void printPossibleLogLevels() {
		System.out.println(PROMPT 
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ LogSetup.getPossibleLogLevels());
	}

	private Level setLevel(String levelString) {
		if (levelString.equals(Level.ALL.toString())) {	
			return Level.ALL;
		} else if (levelString.equals(Level.DEBUG.toString())) {
			return Level.DEBUG;
		} else if (levelString.equals(Level.INFO.toString())) {
			return Level.INFO;
		} else if (levelString.equals(Level.WARN.toString())) {
			return Level.WARN;
		} else if (levelString.equals(Level.ERROR.toString())) {
			return Level.ERROR;
		} else if (levelString.equals(Level.FATAL.toString())) {
			return Level.FATAL;
		} else if (levelString.equals(Level.OFF.toString())) {
			// TODO: Prob need to resort to logging to console from here on out
			return Level.OFF;
		} else {
			return null;
		}
	}
	
    private void printError(String error) {
		System.out.println(PROMPT + "Error! " +  error);
	}
}
