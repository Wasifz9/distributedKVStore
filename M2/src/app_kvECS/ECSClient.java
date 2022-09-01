package app_kvECS;

import java.util.Arrays;
import java.util.Date;

import java.text.SimpleDateFormat;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.PrintWriter;

import java.net.Socket;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import logger.LogSetup;

import app_kvServer.IKVServer.CacheStrategy;

import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;

import exceptions.InvalidMessageException;

public class ECSClient {

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ECSClient> ";
    private static final CacheStrategy DEFAULT_CACHE_STRATEGY = CacheStrategy.LRU;
    private static final int DEFAULT_CACHE_SIZE = 16;

    private BufferedReader stdin;
    private boolean running = true;
    private String rawMetadata = "";

    private String address;
    private int port;
    private Socket clientSocket = null;
    private OutputStream output;
    private InputStream input;

    /**
     * Main entry point for the ECSClient application.
     * 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
            new LogSetup("logs/ecsclient_" + fmt.format(new Date()) + ".log", Level.ALL, true);
            if (args.length != 0) {
                logger.error("Error! Expected 0 arguments");
                logger.error("Usage: ECSClient!");
                System.exit(1);
            }
            ECSClient app = new ECSClient();
            app.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void run() {
        while (isRunning()) {
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

    private void handleCommand(String cmdLine) {
        // TODO: Ensure that certain commands are only accessible AFTER the ZK is
        // initialized
        if (cmdLine.trim().length() == 0) {
            return;
        }

        logger.info("User input: " + cmdLine);
        String[] tokens = cmdLine.split("\\s+");

        if (tokens[0].equals("connect")) {
            if (tokens.length == 3) {
                connectCommand(tokens[1], Integer.parseInt(tokens[2]));
            } else {
                printError("Expected 2 arguments: connect <ECS address> <ECS port>");
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
            } else {
                printError("Expected 0 arguments");
            }
        } else {
            String[] connectedCommands = { "start", "addNodes", "stop", "shutDown", "shutDownECS", "addNode",
                    "removeNode", "list" };

            if (Arrays.asList(connectedCommands).contains(tokens[0])) {
                if (clientSocket == null) {
                    printError("Must connect to ECS first!");
                    return;
                }
            } else {
                printError("Unknown command");
                printHelp();
            }

            if (tokens[0].equals("start")) {
                if (tokens.length == 1) {
                    startCommand();
                } else {
                    printError("Expected 0 arguments");
                }
            } else if (tokens[0].equals("addNodes")) {
                // TODO: Support the input of cache
                if (tokens.length == 2) {
                    addNodesCommand(Integer.parseInt(tokens[1]), DEFAULT_CACHE_STRATEGY.name(),
                            DEFAULT_CACHE_SIZE);
                } else {
                    printError("Expected 1 argument: addNodes <numberOfNodes>");
                }
            } else if (tokens[0].equals("stop")) {
                if (tokens.length == 1) {
                    stopCommand();
                } else {
                    printError("Expected 0 arguments");
                }
            } else if (tokens[0].equals("shutDown")) {
                if (tokens.length == 1) {
                    shutdownCommand();
                } else {
                    printError("Expected 0 arguments");
                }
            } else if (tokens[0].equals("shutDownECS")) {
                if (tokens.length == 1) {
                    shutdownECSCommand();
                } else {
                    printError("Expected 0 arguments");
                }
            } else if (tokens[0].equals("addNode")) {
                if (tokens.length == 1) {
                    addNodesCommand(1, DEFAULT_CACHE_STRATEGY.name(),
                            DEFAULT_CACHE_SIZE);
                } else {
                    printError("Expected 0 arguments");
                }
            } else if (tokens[0].equals("list")) {
                if (tokens.length == 1) {
                    listNodesCommand();
                } else {
                    printError("Expected 0 arguments");
                }
            } else if (tokens[0].equals("removeNode")) {
                if (tokens.length == 2) {
                    removeNodeCommand(tokens[1]);
                } else {
                    printError("Expected 1 argument: removeNode <ZooKeeper path of server>");
                }
            }
        }

    }

    public void connectCommand(String address,
            int port) {
        logger.info("Trying to connect ...");
        try {
            clientSocket = new Socket(address, port);
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();

            logger.info("Connection established to " + address + " on port " + port);
        } catch (Exception e) {
            logger.info("Unable to connect to " + address + " on port " + port);
            clientSocket = null;

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());
        }
    }

    private void addNodesCommand(int count, String cacheStrategy, int cacheSize) {
        try {
            KVMessage msg = new KVMessage("none", String.format("%s,%s,%d", count, cacheStrategy, cacheSize),
                    StatusType.ADD_NODES);
            sendMessage(msg);

            KVMessage res = receiveMessage();
            if (res.getStatus() == StatusType.ACK) {

                System.out.println(PROMPT + "Node(s) added:\n" + res.getValue());
            } else {
                printError("Error while adding nodes");
            }
        } catch (Exception e) {
            logger.error("Error while sending addNodes command");

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());
        }
    }

    private void startCommand() {
        try {
            byte msgBytes[] = { StatusType.START.getVal() };
            KVMessage msg = new KVMessage(msgBytes);
            sendMessage(msg);

            KVMessage res = receiveMessage();
            // TODO: Check for errors here:
            // if (res.getStatus() == StatusType.) {
            // }
        } catch (Exception e) {
            logger.error("Error while sending start command");

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());
        }
    }

    private void stopCommand() {
        try {
            byte msgBytes[] = { StatusType.STOP.getVal() };
            KVMessage msg = new KVMessage(msgBytes);
            sendMessage(msg);

            KVMessage res = receiveMessage();
            // TODO: Check for errors here:
            // if (res.getStatus() == StatusType.) {
            // }
        } catch (Exception e) {
            logger.error("Error while sending stop command");

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());
        }
    }

    private void shutdownCommand() {
        try {
            byte msgBytes[] = { StatusType.SHUTDOWN.getVal() };
            KVMessage msg = new KVMessage(msgBytes);
            sendMessage(msg);

            KVMessage res = receiveMessage();
            // TODO: Check for errors here:
            // if (res.getStatus() == StatusType.) {
            // }
        } catch (Exception e) {
            logger.error("Error while sending shutDown command");

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());
        }
    }

    private void shutdownECSCommand() {
        try {
            byte msgBytes[] = { StatusType.SHUTDOWN_ECS.getVal() };
            KVMessage msg = new KVMessage(msgBytes);
            sendMessage(msg);
            setRunning(false);
        } catch (Exception e) {
            logger.error("Error while sending shutDownECS command");

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());
        }
    }

    private void removeNodeCommand(String serverPath) {
        try {
            KVMessage msg = new KVMessage("none", serverPath,
                    StatusType.REMOVE_NODE);
            sendMessage(msg);

            KVMessage res = receiveMessage();
            if (res.getStatus() == StatusType.ACK) {
                System.out.println(PROMPT + "Successfully deleted " + serverPath);
            } else {
                printError("Could not remove:" + serverPath);
                printError("Please make sure the node exists & is running!");
            }
        } catch (Exception e) {
            logger.error("Error while sending removeNode command");

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());
        }
    }

    private void listNodesCommand() {
        try {
            byte msgBytes[] = { StatusType.LIST.getVal() };
            KVMessage msg = new KVMessage(msgBytes);
            sendMessage(msg);

            KVMessage res = receiveMessage();
            if (res.getStatus() == StatusType.ACK) {
                System.out.println(PROMPT + "Servers:\n" + res.getValue().replace(",", "\n"));
            } else {
                printError("Error while listing nodes");
            }
        } catch (Exception e) {
            logger.error("Error while sending listNodes command");

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());
        }
    }

    private void quitCommand() {
        try {
            if (clientSocket != null) {
                input.close();
                output.close();
                clientSocket.close();
                clientSocket = null;
            }
            setRunning(false);
        } catch (Exception e) {
            printError("Error while quitting ECSClient!");

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());
        }
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public boolean isRunning() {
        return running;
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECS CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");

        sb.append(PROMPT).append("connect <ECS Address> <ECS Port>");
        sb.append(
                "\t\t connect to the ECS server at <ECS Address> on port <ECS port>.\n");

        sb.append(PROMPT).append("addNode");
        sb.append(
                "\t\t create a new KVServer and add it to the storage service at an arbitrary position.\n");

        sb.append(PROMPT).append("addNodes <numberOfNodes>");
        sb.append(
                "\t\t randomly choose <numberOfNodes> servers from the available machines and start the KVServer by issuing an SSH call to the respective machine. This call launches the storage server. For simplicity, locate the KVServer.jar in the same directory as the ECS. All storage servers are initialized with the metadata and any persisted data, and remain in state stopped.\n");

        sb.append(PROMPT).append("start");
        sb.append(
                "\t\t starts the storage service by calling start() on all KVServer instances that participate in the service.\n");

        sb.append(PROMPT).append("stop");
        sb.append(
                "\t\t stops the service; all participating KVServers are stopped for processing client requests but the processes remain running.\n");

        sb.append(PROMPT).append("shutDown");
        sb.append(
                "\t\t stops the service; all participating KVServers are stopped for processing client requests but the processes remain running. And shuts down the ECS server.\n");

        sb.append(PROMPT).append("removeNode <server name>");
        sb.append("\t\t remove a server from the storage service at an arbitrary position.\n");

        sb.append(PROMPT).append("list");
        sb.append("\t\t shows a list of all the servers (active + inactive).\n");

        sb.append(PROMPT).append("shutDownECS");
        sb.append("\t\t stops all server instances and exits the remote processes.\n");

        sb.append(PROMPT).append("logLevel <level>");

        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit");
        sb.append("\t\t exits the program");

        sb.append(PROMPT).append("help");
        sb.append("\t\t shows this help menu\n");

        System.out.println(sb.toString());
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    private KVMessage receiveMessage()
            throws IOException, InvalidMessageException, Exception {

        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        // logger.debug("First read:" + read);

        if (read == -1) {
            throw new Exception("Reached end of stream!");
        }

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

        // logger.debug("Last read:" + read);

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

        logger.info("RECEIVE: STATUS="
                + msg.getStatus() + " KEY=" + msg.getKey() + " VALUE=" + msg.getValue());

        return msg;
    }

    private void sendMessage(KVMessage msg) throws IOException {
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();

        logger.info("SEND: STATUS="
                + msg.getStatus() + " KEY=" + msg.getKey() + " VALUE=" + msg.getValue());

    }
}
