package testing;

import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvECS.ECSClient;
import app_kvECS.ECS;
import app_kvServer.IKVServer;
import app_kvServer.IKVServer.Status;

import java.io.File;
import java.util.Map;
import java.util.Collection;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

import logger.LogSetup;

import app_kvServer.KVServer;
import app_kvServer.IKVServer.CacheStrategy;
import app_kvServer.IKVServer.Status;
import client.KVStore;
import shared.messages.IKVMessage;
import ecs.ECSNode;

public class AllTests {
	private static KVServer kvserver;
	private static ECSClient client;
	private static int PORT = 50000;
	private static int ECSPORT = 8000; 
	private static int zkPort = 2181; 
	private static int serverPort = 50019; 
	private static Logger logger;
	private static ECS ECSServer; 

	private static String configPath = "";


	// TODO - in teardown remove root node in zookeeper
	static {
		try {
			File file = new File("logs/testing/test.log");
			file.delete();

			new LogSetup("logs/testing/test.log", Level.INFO);
			logger = Logger.getRootLogger();
			logger.debug("testing boot//.");

			ECSServer = new ECS(ECSPORT, zkPort, configPath); 
			logger.info("hidkaha");
			Runnable server = new Runnable() {
				@Override
				public void run() {
					logger.debug("Hello");
					ECSServer.testrun();
					ECSServer.addNode("any",20);
					logger.debug("Hello");
					ECSServer.start();
					logger.debug("OH noo");
					// need KVStore to call the right server
					KVStore kvClient = new KVStore("localhost", serverPort);
					try {
						logger.debug("Hello");
						kvClient.connect();
						kvClient.put("haha", "10");
						IKVMessage val = kvClient.get("haha");
						logger.debug(val.getValue());
					} catch (Exception e) {
						logger.debug("TES: CONNECt bricked");
						logger.debug(e.getMessage());
						ECSServer.shutdown();
					}
						logger.error("herer:" + kvserver.getStatus());
						ECSServer.shutdown();
					// kvserver.run();
				}
			};
			// new Thread(server).start();
		
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Test suite() {

		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		logger.info("Starting test: ");
		// ECS.ece = ECSServer;
		// clientSuite.addTestSuite(ECSTest.class);
		// logger.debug("Hello");
		ECSServer.testrun();
		// ECSServer.addNode("ant", 20);
		ECSServer.addNodes(10,"any",20);
		// // ECSServer.start();

		try {
			logger.info("sleeping");
			Thread.currentThread().sleep(2000);
			ECSServer.start();
		} catch (Exception e) {

			logger.info("error sleep: " + e.getMessage());
		}
		// // need KVStore to call the right server
		// KVStore kvClient = new KVStore("localhost", serverPort);
		// try {
		// 	logger.debug("KV client connect in progress...");
		// 	kvClient.connect();
	
		// 	// logger.info("sleeping");
		// 	// Thread.currentThread().sleep(5000);

		// 	kvClient.put("haha", "10");
		// 	IKVMessage val = kvClient.get("haha");
		// 	logger.debug(val.getValue());
		// } catch (Exception e) {
		// 	logger.error(e);
		// 	logger.debug("TES: CONNECt bricked");
		// 	logger.debug(e.getMessage());
			
		// }
		// ECSServer.shutdown();

		// try{
		// 	logger.info("sleeping");
		// 	do {
		// 	Thread.currentThread().sleep(1000);
		// 	} while (true)
		// } catch(Exception e) {
		// 	logger.error(e);
		// }
		
		// logger.error("herer:" + kvserver.getStatus());
		// ConnectionTest.port = 50019;
		// ConnectionTest.server = ECSServer;
		// clientSuite.addTestSuite(ConnectionTest.class);

		PerformanceBasicTest.port = 50019;
		PerformanceBasicTest.server = ECSServer;
		clientSuite.addTestSuite(PerformanceBasicTest.class);

		// clientSuite.addTestSuite(ECSTest.class)
		// BasicTest.server = ECSServer;
		// BasicTest.port = 50019;
		// clientSuite.addTestSuite(BasicTest.class);

		// RequestTest.server = kvserver;
		// RequestTest.port = PORT;
		// clientSuite.addTestSuite(RequestTest.class);

		// ConcurrencyBasicTest.server = kvserver;
		// ConcurrencyBasicTest.port = PORT;
		// clientSuite.addTestSuite(ConcurrencyBasicTest.class);

		// ConcurrencyHardTest.server = kvserver;
		// ConcurrencyHardTest.port = PORT;
		// clientSuite.addTestSuite(ConcurrencyHardTest.class);
		// clientSuite.addTestSuite(CacheTest.class);

		// boolean valu = client.shutdown();
		// try {
		// 	Thread.currentThread().sleep(15000);
		// } catch (Exception e) {
		// 	logger.debug(e.getMessage());
		// }
	 	
		// ECSServer.shutdown();
		return clientSuite;
	}
	// TODO Write a test case to check to make sure connection times out for a
	// response
}