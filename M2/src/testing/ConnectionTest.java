package testing;

import java.net.UnknownHostException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.KVServer;
import app_kvServer.IKVServer.Status;
import junit.framework.TestCase;
import client.KVStore;
import app_kvECS.ECS;

public class ConnectionTest extends TestCase {

	public static int port;
	private KVStore kvClient;
	public static ECS server; 
	private static Logger logger = Logger.getRootLogger();

	public void setUp() {
		// server.setStatus(Status.STARTED);
		logger.info("ConnectTest - Starting setup");
		// server.testrun();
		// server.addNode("any",20);
		// logger.debug("Hello");
		// try {
		// 	logger.info("sleeping");
		// 	Thread.currentThread().sleep(2000);
		// 	server.start();
		// } catch (Exception e) {

		// 	logger.info("error sleep: " + e.getMessage());
		// }
		logger.info("Starting...");
		kvClient = new KVStore("localhost", port);
		try {
			kvClient.connect();
			logger.info("done connection...");
		} catch (Exception e) {
			logger.error(e);
		}
	}

	public void tearDown() {
		kvClient.disconnect();
		// Boolean val = server.shutdown();
		// try {
		// 	logger.info("sleeping");
		// 	Thread.currentThread().sleep(500);
		// } catch (Exception e) {
		// 	logger.info("error sleep: " + e.getMessage());
		// }
		// Boolean val = server.shutdown();
		// logger.info("ConnectTest - ECS SHUTDOWN STATUS: " + val);
	}

	public void testConnectionSuccess() {
		Exception ex = null;

		KVStore kvClient = new KVStore("localhost", port);
		try {
			logger.info("Inside ConnectTest: start test");
			kvClient.connect();
			logger.info("Inside ConnectTest: done connect");
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
	}


	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", port);
		kvClient.test = true;

		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}

		kvClient.test = false;
		assertTrue(ex instanceof UnknownHostException);
	}

	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 123456789);
		kvClient.test = true;

		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}

		kvClient.test = false;

		logger.info("illegal port: " + ex);
		assertTrue(ex instanceof IllegalArgumentException);
	}

	public void testHeartBeat() {
		long first = (long) 0;
		long second = (long) 0;
		Exception ex = null;

		try {
			kvClient.connect();
			first = kvClient.getLastResponse();
			Thread.sleep(11000);
			second = kvClient.getLastResponse();

		} catch (Exception e) {
			ex = e;
		}
		assertTrue(first != second);
	}

}
