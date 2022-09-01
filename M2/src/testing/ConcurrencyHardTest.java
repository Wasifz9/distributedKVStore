package testing;

import java.io.File;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import app_kvServer.KVServer;
import app_kvServer.ConcurrentNode;

import shared.messages.KVMessage;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

import client.KVStore;

import testing.helpers.GetRunnable;
import testing.helpers.IResponseRunnable;
import testing.helpers.PutRunnable;

import exceptions.InvalidMessageException;

public class ConcurrencyHardTest extends TestCase {

	private final String STORAGE_DIRECTORY = "storage/";

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
	
	public void testConcurrentDelete() {
		final int NUM_CONNECTIONS = 5;
		logger.info("====TEST " + NUM_CONNECTIONS + " concurrent DELETES====");

		final String KEY = "key";
		String value = "woah";

		IKVMessage response = null;
		Exception ex = null;
		try {
			response = kvClient.put(KEY, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertTrue(new File(STORAGE_DIRECTORY + KEY).isFile());
		assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
		assertTrue(response.getKey().equals(KEY));
		assertTrue(response.getValue().equals(value));
		
		// Start the delete
		value = "null";
		server.wait = true;

		logger.info("======Thread! Spawning======");
		IResponseRunnable[] values = new IResponseRunnable[NUM_CONNECTIONS];
		Thread[] threads = new Thread[NUM_CONNECTIONS];
		for (int i = 0; i < NUM_CONNECTIONS; ++i) {
			values[i] = new PutRunnable(port, KEY, false, value, false);
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

		int deleterID = server.getClientRequests().get(KEY).peek()[0];
		logger.info("Deletor:" + deleterID);

		server.wait = false;

		try {
			for (int i = 0; i < NUM_CONNECTIONS; ++i) {
				threads[i].join();
				IKVMessage tResponse = values[i].getResponse();
				assertTrue(tResponse.getKey().equals(KEY));
				logger.info("Thread " + threads[i].getId() + " got:" + tResponse.getStatus());
				if (values[i].getID() == deleterID) {
					assertTrue(tResponse.getStatus() == StatusType.DELETE_SUCCESS);
				} else {
					assertTrue(tResponse.getStatus() == StatusType.DELETE_ERROR);
				}
			}
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);

		try {
			response = kvClient.get(KEY);
			// Wait for pruning to complete:
			Thread.sleep(500);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertFalse(new File(STORAGE_DIRECTORY + KEY).isFile());
		assertTrue(response.getStatus() == StatusType.GET_ERROR);
		assertTrue(response.getKey().equals(KEY));
	}

	// Test case: WRRRDR, W = WRITE, R = READ, D = DELETE
	public void testContention1() {
		final String TEST_CASE = "WRRRDR";
		final int NUM_CONNECTIONS = TEST_CASE.length();
		logger.info("====TEST concurrent contention 1: " + TEST_CASE + "(" + NUM_CONNECTIONS + " connections)====");

		final String KEY = "contention1";
		final String VALUE = "woah";

		IKVMessage response = null;
		Exception ex = null;
		
		// Construct the test runnables
		// Test: WRRRDR
		int i = 0;
		IResponseRunnable[] values = new IResponseRunnable[NUM_CONNECTIONS];
		values[i++] = new PutRunnable(port, KEY, false, VALUE, false);
		values[i++] = new GetRunnable(port, KEY);
		values[i++] = new GetRunnable(port, KEY);
		values[i++] = new GetRunnable(port, KEY);
		values[i++] = new PutRunnable(port, KEY, false, "null", false);
		values[i++] = new GetRunnable(port, KEY);
		assertTrue(i == NUM_CONNECTIONS);
		
		i = 0;
		IKVMessage[] expected = new KVMessage[NUM_CONNECTIONS];
		try {
			expected[i++] = new KVMessage(KEY, VALUE, StatusType.PUT_SUCCESS);
			expected[i++] = new KVMessage(KEY, VALUE, StatusType.GET_SUCCESS);
			expected[i++] = new KVMessage(KEY, VALUE, StatusType.GET_SUCCESS);
			expected[i++] = new KVMessage(KEY, VALUE, StatusType.GET_SUCCESS);
			expected[i++] = new KVMessage(KEY, "null", StatusType.DELETE_SUCCESS);
			expected[i++] = new KVMessage(KEY, null, StatusType.GET_ERROR);
		} catch (InvalidMessageException ime) {
			ex = ime;
		}
		assertNull(ex);
		assertTrue(i == NUM_CONNECTIONS);
		
		server.wait = true;
		// Start the threads
		logger.info("======Thread! Spawning======");
		Thread[] threads = new Thread[NUM_CONNECTIONS];
		for (i = 0; i < NUM_CONNECTIONS; ++i) {
			threads[i] = new Thread(values[i]);
			threads[i].start();
		}

		while (!server.inQueue(KEY));
		while (server.getClientRequests().get(KEY).len() != NUM_CONNECTIONS);
		
		logger.info("Fixing the Queue to:" + TEST_CASE);

		ConcurrentNode node = server.getClientRequests().get(KEY);
		ArrayList<int[]> oldQ = new ArrayList<int[]>(node.getQ());
		ConcurrentLinkedQueue<int[]> newQ = new ConcurrentLinkedQueue<int[]>();

		logger.info("Queue now: " + node.printQ());

		for (i = 0; i < NUM_CONNECTIONS; ++i) {
			for (int j = 0; j < NUM_CONNECTIONS; ++j) {
				if (values[i].getID() == oldQ.get(j)[0]) {
					newQ.add(oldQ.get(j));
				}
			}
		}
		node.setQ(newQ);
		logger.info("Debug queue after: " + node.printQ());
		
		server.setClientRequest(KEY, node);
		logger.info("Queue after: " + server.getClientRequests().get(KEY).printQ());

		server.wait = false;

		try {
			for (i = 0; i < NUM_CONNECTIONS; ++i) {
				threads[i].join();
				assertTrue(values[i].getResponse().equal(expected[i]));
			}
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertFalse(new File(STORAGE_DIRECTORY + KEY).isFile());
	}
}
