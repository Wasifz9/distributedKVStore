package testing;

import java.net.UnknownHostException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.KVServer;
import junit.framework.TestCase;
import client.KVStore;

public class ConnectionTest extends TestCase {

	public static int port;
	private KVStore kvClient;
	public static KVServer server;
	private static Logger logger = Logger.getRootLogger();

	public void setUp() {
		kvClient = new KVStore("localhost", port);
		try {
			kvClient.connect();
		} catch (Exception e) {
			logger.error(e);
		}
	}

	public void tearDown() {
		kvClient.disconnect();
		server.clearStorage();
	}

	public void testConnectionSuccess() {
		Exception ex = null;

		KVStore kvClient = new KVStore("localhost", port);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
	}

	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", port);

		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex instanceof UnknownHostException);
	}

	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 123456789);

		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}

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
