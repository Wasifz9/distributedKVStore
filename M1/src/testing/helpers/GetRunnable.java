package testing.helpers;

import org.apache.log4j.Logger;

import shared.messages.IKVMessage;

import client.KVStore;

import testing.helpers.IResponseRunnable;

public class GetRunnable implements IResponseRunnable {

    private static Logger logger = Logger.getRootLogger();

    private volatile IKVMessage response;
    private volatile int id;
    private volatile String key;
    private volatile int port;

    public GetRunnable(int port, String key) {
        this.key = key;
        this.port = port;
    }

    @Override
    public void run() {
        try {
            Logger logger = Logger.getRootLogger();
            logger.info("======Thread! START======");
            KVStore kv = new KVStore("localhost", port);
            kv.connect();
            this.id = kv.output_port;
            IKVMessage response = kv.get(key);
            logger.info("======Thread! DONE======");
            logger.info("======Thread! Get: " + response.print() + "======");
            this.response = response;
            kv.disconnect();
        } catch (Exception e) {
            logger.error("Error in Thread:" + Thread.currentThread().getId() + ": " + e);
        }
    }

    @Override
    public IKVMessage getResponse() {
        return this.response;
    }

    @Override
    public int getID() {
        return this.id;
    }
}
