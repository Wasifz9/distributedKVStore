package testing.helpers;

import org.apache.log4j.Logger;

import shared.messages.IKVMessage;

import client.KVStore;

import testing.helpers.IResponseRunnable;

public class PutRunnable implements IResponseRunnable {

    private static Logger logger = Logger.getRootLogger();

    private volatile IKVMessage response;
    private volatile int id;
    private volatile String key;
    private volatile String value;
    private volatile int port;
    private volatile boolean prefix_key;
    private volatile boolean prefix_value;

    public PutRunnable(int port, String key, boolean prefix_key, String value, boolean prefix_value) {
        this.port = port;
        this.key = key;
        this.prefix_key = prefix_key;
        this.value = value;
        this.prefix_value = prefix_value;
    }

    @Override
    public void run() {
        try {
            Logger logger = Logger.getRootLogger();
            logger.info("======Thread! START======");
            KVStore kv = new KVStore("localhost", port);
            kv.connect();
            this.id = kv.output_port;
            String put_key = this.prefix_key ? this.key + this.id : this.key;
            String put_val = this.prefix_value ? this.value + this.id : this.value;
            IKVMessage response = kv.put(put_key, put_val);
            logger.info("======Thread! DONE======");
            logger.info("======Thread! Put: " + response.print() + "======");
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