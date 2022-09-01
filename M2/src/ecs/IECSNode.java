package ecs;

import app_kvServer.IKVServer.Status;

public interface IECSNode {

    public enum NodeEvent {
        BOOT, METADATA, START, COPY, COPY_COMPLETE, MOVE, STOP, SHUTDOWN, CRASH, CRASH_COMPLETE
    };

    /**
     * @return initialize the server
     */
    public boolean initServer();

    /**
     * @return the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName();

    /**
     * @return the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost();

    /**
     * @return the port number of the node (ie 8080)
     */
    public int getNodePort();

    /**
     * @return array of two strings representing the low and high range of the
     *         hashes that the given
     *         node is responsible for
     */
    public String[] getNodeHashRange();

    public void setStatus(Status status);

    public Status getStatus();

}
