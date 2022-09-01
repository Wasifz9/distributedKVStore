package ecs;

import java.util.Date;

import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import logger.LogSetup;

import app_kvServer.IKVServer.Status;

public class ECSNode implements IECSNode {

    private static Logger logger = Logger.getRootLogger();

    private String name;
    private String host;
    private int port;
    private int zkPort = -1;
    private String ECSIP;
    private String[] hashRange;
    private Status status = Status.ADDED;

    public ECSNode(String name, String host, int port, int zkPort, String[] hashRange, String ECSIP) {
        try {
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
            new LogSetup("logs/ecsnode_" + name + "_" + fmt.format(new Date()) + ".log", Level.ALL, false);
        } catch (Exception e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
        }

        this.name = name;
        this.host = host;
        this.port = port;
        this.zkPort = zkPort;
        this.ECSIP = ECSIP;
        this.hashRange = hashRange.clone();
    }

    public ECSNode(String name, String host, int port, String[] hashRange, String ECSIP) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.ECSIP = ECSIP;
        this.hashRange = hashRange.clone();
    }

    public boolean initServer() {
        if (this.zkPort == -1) {
            return false;
        }

        logger.info("Intializing server ... \nRunning script ...");
        String script = "script.sh";

        Runtime run = Runtime.getRuntime();
        String[] envp = { "host=" + this.host, "name=" + this.name, "port=" + this.port,
                "zkPort=" + this.zkPort, "ECS_host=" +
                        this.ECSIP, "isLoadReplica=" + false, "parentName=" + ""
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

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    // Return a string representing this node's data
    public String getMeta() {
        // TODO: See if name is required
        return String.format("%s:%s:%s:%s:%s", name, host, port, hashRange[0], hashRange[1]);
    }

    /**
     * @return the name of the node (ie "Server0")
     */
    @Override
    public String getNodeName() {
        return name;
    }

    /**
     * @return the hostname of the node (ie "8.8.8.8")
     */
    @Override
    public String getNodeHost() {
        return host;
    }

    /**
     * @return the port number of the node (ie 8080)
     */
    @Override
    public int getNodePort() {
        return port;
    }

    public int getZKPort() {
        return zkPort;
    }

    public void setNodeHashRange(String[] hashRange) {
        this.hashRange = hashRange.clone();
    }

    /**
     * @return array of two strings representing the low and high range of the
     *         hashes that the given
     *         node is responsible for
     */
    @Override
    public String[] getNodeHashRange() {
        return hashRange;
    }

    @Override
    public void setStatus(Status status) {
        this.status = status;
    }

    @Override
    public Status getStatus() {
        return status;
    }

}
