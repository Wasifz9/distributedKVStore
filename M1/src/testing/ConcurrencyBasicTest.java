package testing;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import app_kvServer.KVServer;

import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

import client.KVStore;

import testing.helpers.IResponseRunnable;
import testing.helpers.GetRunnable;
import testing.helpers.PutRunnable;

public class ConcurrencyBasicTest extends TestCase {

	private KVStore kvClient;
	private static Logger logger = Logger.getRootLogger();
	public static KVServer server;
	public static int port;

	public void setUp() {
		kvClient = new KVStore("localhost", port);
		try {
			server.clearStorage();
			kvClient.connect();
		} catch (Exception e) {
			logger.error(e);
		}
	}

	public void tearDown() {
		kvClient.disconnect();
		server.clearStorage();
	}

	public void testConcurrentGet() {
		final int NUM_CONNECTIONS = 5;
		logger.info("====TEST " + NUM_CONNECTIONS + " concurrent GETS====");

		final String KEY = "key";
		final String VALUE = "woah";
		IKVMessage response = null;
		Exception ex = null;
		try {
			response = kvClient.put(KEY, VALUE);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
		assertTrue(response.getKey().equals(KEY));
		assertTrue(response.getValue().equals(VALUE));

		server.wait = true;

		logger.info("======Thread! Spawning======");
		IResponseRunnable[] values = new IResponseRunnable[NUM_CONNECTIONS];
		Thread[] threads = new Thread[NUM_CONNECTIONS];
		for (int i = 0; i < NUM_CONNECTIONS; ++i) {
			values[i] = new GetRunnable(port, KEY);
			threads[i] = new Thread(values[i]);
			threads[i].start();
		}

		while (!server.inStorage(KEY) ||
				server.getClientRequests().get(KEY).len() != NUM_CONNECTIONS);

		server.wait = false;

		try {
			for (int i = 0; i < NUM_CONNECTIONS; ++i) {
				threads[i].join();
				IKVMessage tResponse = values[i].getResponse();
				assertTrue(tResponse.getStatus() == StatusType.GET_SUCCESS);
				assertTrue(tResponse.getKey().equals(KEY));
				assertTrue(tResponse.getValue().equals(VALUE));
			}
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
	}

	public void testConcurrentPutDiffKeys() {
		final int NUM_CONNECTIONS = 5;
		logger.info("====TEST " + NUM_CONNECTIONS + " concurrent PUTS (Different keys)====");

		final String KEY_PREFIX = "key_";
		final String VALUE = "foo";
		
		server.wait = true;

		logger.info("======Thread! Spawning======");
		IResponseRunnable[] values = new IResponseRunnable[NUM_CONNECTIONS];
		Thread[] threads = new Thread[NUM_CONNECTIONS];
		for (int i = 0; i < NUM_CONNECTIONS; ++i) {
			values[i] = new PutRunnable(port, KEY_PREFIX, true, VALUE, false);
			threads[i] = new Thread(values[i]);
			threads[i].start();
		}

		server.wait = false;

		Exception ex = null;
		try {
			for (int i = 0; i < NUM_CONNECTIONS; ++i) {
				threads[i].join();
				IKVMessage tResponse = values[i].getResponse();
				assertTrue(server.inStorage(KEY_PREFIX + values[i].getID()));
				assertTrue(tResponse.getStatus() == StatusType.PUT_SUCCESS);
				assertTrue(tResponse.getKey().equals(KEY_PREFIX + values[i].getID()));
				assertTrue(tResponse.getValue().equals(VALUE));
			}
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
	}

	public void testConcurrentPutSameKeys() {
		final int NUM_CONNECTIONS = 5;
		logger.info("====TEST " + NUM_CONNECTIONS + " concurrent PUTS (Same keys)====");

		final String KEY = "key";
		final String VALUE_PREFIX = "val_";

		server.wait = true;

		logger.info("======Thread! Spawning======");
		IResponseRunnable[] values = new IResponseRunnable[NUM_CONNECTIONS];
		Thread[] threads = new Thread[NUM_CONNECTIONS];
		for (int i = 0; i < NUM_CONNECTIONS; ++i) {
			values[i] = new PutRunnable(port, KEY, false, VALUE_PREFIX, true);
			threads[i] = new Thread(values[i]);
			threads[i].start();
		}

		while (!server.inQueue(KEY));

		logger.info("Sleeping for a bit");
		try {
			Thread.sleep(2000);
		} catch (InterruptedException ie) {
			fail("InterruptedException thrown!");
		}

		while (server.getClientRequests().get(KEY).len() != NUM_CONNECTIONS);

		int keyCreator = server.getClientRequests().get(KEY).peek()[0];

		server.wait = false;

		Exception ex = null;
		try {
			for (int i = 0; i < NUM_CONNECTIONS; ++i) {
				threads[i].join();
				IKVMessage tResponse = values[i].getResponse();
				
				if (values[i].getID() == keyCreator) {
					assertTrue(tResponse.getStatus() == StatusType.PUT_SUCCESS);
				} else {
					assertTrue(tResponse.getStatus() == StatusType.PUT_UPDATE);
				}

				assertTrue(tResponse.getKey().equals(KEY));
				assertTrue(tResponse.getValue().equals(VALUE_PREFIX + values[i].getID()));
			}
			assertTrue(server.inStorage(KEY));
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
	}
}
