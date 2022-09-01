package testing;

import org.junit.Test;

import junit.framework.TestCase;
import logger.LogSetup;

import java.util.Queue;
import java.util.ArrayList;
import java.util.concurrent.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.KVServer;
import app_kvECS.ECS;
import app_kvServer.ConcurrentNode;

import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

import client.KVStore;
import testing.helpers.IResponseRunnable;

public class PerformanceBasicTest extends TestCase {

	private KVStore kvClient;
	private static Logger logger = Logger.getRootLogger();
	public static ECS server;
	public static int port;

	// public void setUp() {
	// kvClient = new KVStore("localhost", port);
	// try {
	// kvClient.connect();
	// } catch (Exception e) {
	// logger.error(e);
	// }
	// }

	// public void tearDown() {
	// kvClient.disconnect();
	// // server.clearStorage();
	// }

	// public void testConcurrentPutSameKeys() {
	// final int NUM_CONNECTIONS = 5;
	// final String KEY = "key";
	// final String VALUE_PREFIX = "val_";
	// logger.info("====TEST " + NUM_CONNECTIONS + " concurrent PUTS (Same
	// keys)====");

	// class ResponseRunnable implements IResponseRunnable {
	// private volatile IKVMessage response;
	// private volatile int id;

	// @Override
	// public void run() {
	// try {
	// Logger logger = Logger.getRootLogger();
	// logger.info("======Thread! START======");
	// KVStore kv = new KVStore("localhost", port);
	// kv.connect();
	// this.id = kv.output_port;
	// IKVMessage response = kv.put(KEY, VALUE_PREFIX + this.id);
	// logger.info("======Thread! DONE======");
	// logger.info("======Thread! Response: " + response.print() + "======");
	// this.response = response;
	// kv.disconnect();
	// } catch (Exception e) {
	// logger.error("Error in Thread:" + this.id + ": " + e);
	// }
	// }

	// @Override
	// public IKVMessage getResponse() {
	// return this.response;
	// }

	// @Override
	// public int getID() {
	// return this.id;
	// }
	// }

	// server.wait = true;

	// logger.info("======Thread! Spawning======");
	// ResponseRunnable[] values = new ResponseRunnable[NUM_CONNECTIONS];
	// Thread[] threads = new Thread[NUM_CONNECTIONS];
	// for (int i = 0; i < NUM_CONNECTIONS; ++i) {
	// values[i] = new ResponseRunnable();
	// threads[i] = new Thread(values[i]);
	// threads[i].start();
	// }

	// while (!server.inQueue(KEY)) {
	// }
	// ;

	// logger.info("Sleeping for a bit");
	// try {
	// Thread.sleep(2000);
	// } catch (InterruptedException ie) {
	// fail("InterruptedException thrown!");
	// }

	// while (server.getClientRequests().get(KEY).len() != NUM_CONNECTIONS) {
	// }
	// ;

	// int keyCreator = server.getClientRequests().get(KEY).peek()[0];

	// server.wait = false;

	// Exception ex = null;
	// try {
	// for (int i = 0; i < NUM_CONNECTIONS; ++i) {
	// threads[i].join();
	// IKVMessage tResponse = values[i].getResponse();

	// if (values[i].getID() == keyCreator) {
	// assertTrue(tResponse.getStatus() == StatusType.PUT_SUCCESS);
	// } else {
	// assertTrue(tResponse.getStatus() == StatusType.PUT_UPDATE);
	// }

	// assertTrue(tResponse.getKey().equals(KEY));
	// assertTrue(tResponse.getValue().equals(VALUE_PREFIX + values[i].getID()));
	// }
	// assertTrue(server.inStorage(KEY));
	// } catch (Exception e) {
	// ex = e;
	// }

	// assertNull(ex);
	// }

	public void testThroughput() {

		Runnable workload = new Runnable() {

			@Override
			public void run() {
				try {
					int test = 50019;
					int index = (int) Thread.currentThread().getId() % 10;
					logger.info("Thread " + "" + (int) Thread.currentThread().getId() + " - " + index);
					KVStore kv = new KVStore("localhost", test + index);
					Logger logger = Logger.getRootLogger();
					kv.connect();

					String key = "key" + "" + (int) Thread.currentThread().getId();
					for (int i = 0; i < 500; ++i) {
						if (i % 2 == 0) {
							kv.put(key, "" + (int) Thread.currentThread().getId());
						} else {
							kv.get(key);
						}
						Thread.currentThread().sleep(500);
					}

					kv.disconnect();
				} catch (Exception e) {
					Exception ex = e;
					e.printStackTrace();
					logger.error("Error in THREADS:" + ex);
				}
			}
		};

		int t_count = 1;
		Thread[] tarr = new Thread[t_count];

		for (int i = 0; i < t_count; ++i) {
			tarr[i] = new Thread(workload);
			tarr[i].start();
		}

		for (int i = 0; i < t_count; ++i) {
			try {
				logger.info("Trying to join thread: " + i);
				tarr[i].join();
				logger.info("DONE joining thread: " + i);
			} catch (Exception e) {
				logger.error(e);
			}
		}
		server.shutdown();
		logger.info("DONE PERF TEST !!!!!");


	}

	// public void testLatency() {
	// long Put2080[] = new long[100];
	// long Put8020[] = new long[100]; // 80 get 20 put
	// long Put5050[] = new long[100];
	// long Get2080[] = new long[100];
	// long Get8020[] = new long[100]; // 80 get 20 put
	// long Get5050[] = new long[100];

	// // testing
	// String key = "foo";
	// String value = "bar";

	// // place holder to be over written by many start times
	// long startTime;

	// // initialize the test value in storage
	// try {
	// kvClient.put(key, value);
	// } catch (Exception e) {
	// }

	// // 20 get 80 put
	// for (int i = 0; i < 100; i++) {
	// if (i % 5 == 0) {
	// startTime = System.nanoTime();

	// try {
	// kvClient.get(key);
	// } catch (Exception e) {
	// }

	// Get2080[i] = System.nanoTime() - startTime;
	// } else {
	// startTime = System.nanoTime();

	// try {
	// kvClient.put(key, value);
	// } catch (Exception e) {
	// }

	// Put2080[i] = System.nanoTime() - startTime;
	// }
	// }

	// // 80 get 20 put
	// for (int i = 0; i < 100; i++) {
	// if (i % 5 == 0) {
	// startTime = System.nanoTime();
	// try {
	// kvClient.put(key, value);
	// } catch (Exception e) {
	// }
	// Put8020[i] = System.nanoTime() - startTime;
	// logger.info("8020: put value" + Put8020[i]);
	// } else {
	// startTime = System.nanoTime();

	// try {
	// kvClient.get(key);
	// } catch (Exception e) {
	// }
	// Get8020[i] = System.nanoTime() - startTime;
	// logger.info("8020: get value" + Get8020[i]);
	// }

	// }

	// // Get50Put50
	// for (int i = 0; i < 100; i++) {
	// if (i % 2 == 0) {
	// startTime = System.nanoTime();
	// try {
	// kvClient.get(key);
	// } catch (Exception e) {
	// }
	// Get5050[i] = System.nanoTime() - startTime;
	// } else {
	// startTime = System.nanoTime();
	// try {
	// kvClient.put(key, value);
	// } catch (Exception e) {
	// }
	// Put5050[i] = System.nanoTime() - startTime;
	// }
	// }

	// // Calculations for averages
	// long total = 0;

	// for (int i = 0; i < Put2080.length; i++) {
	// total = total + Put2080[i];
	// }
	// double Put2080average = total / Put2080.length;

	// total = 0;

	// for (int i = 0; i < Get2080.length; i++) {
	// total = total + Get2080[i];
	// }
	// double Get2080average = total / Get2080.length;

	// total = 0;

	// for (int i = 0; i < Put8020.length; i++) {
	// total = total + Put8020[i];
	// }
	// double Put8020average = total / Put8020.length;

	// total = 0;

	// for (int i = 0; i < Get8020.length; i++) {
	// total = total + Get8020[i];
	// }
	// double Get8020average = total / Get8020.length;

	// total = 0;

	// for (int i = 0; i < Put5050.length; i++) {
	// total = total + Put5050[i];
	// }
	// double Put5050average = total / Put5050.length;

	// total = 0;

	// for (int i = 0; i < Get5050.length; i++) {
	// total = total + Get5050[i];
	// }
	// double Get5050average = total / Get5050.length;

	// logger.info("20 Get 80 Put | Put latency: " + Put2080average);
	// logger.info("20 Get 80 Put | Get latency: " + Get2080average);
	// logger.info("80 Get 20 Put | Put latency: " + Put8020average);
	// logger.info("80 Get 20 Put | Get latency: " + Get8020average);
	// logger.info("50 Get 50 Put | Put latency: " + Put5050average);
	// logger.info("50 Get 50 Put | Get latency: " + Get5050average);
	// }
}
