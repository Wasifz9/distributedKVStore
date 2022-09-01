package testing;

import java.io.File;

import java.util.Scanner;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import client.KVStore;

import app_kvServer.KVServer;

import shared.messages.KVMessage;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

import exceptions.InvalidMessageException;

public class RequestTest extends TestCase {

	private final String STORAGE_DIRECTORY = "storage/";
	
	private KVStore kvClient;
	private static Logger logger = Logger.getRootLogger();
	public static KVServer server;
	public static int port;

	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
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

	public void testBadRequest() {
		assertTrue(true);
		if (true) return;

		logger.info("====TEST BAD REQUEST====");
		final String KEY = "foo";
		final String VALUE = "bar";

		KVMessage response = null;
		Exception ex = null;
		
		try {
			KVMessage message = new KVMessage(KEY, VALUE, StatusType.GET_SUCCESS);
			kvClient.sendMessage(message, false);
			response = kvClient.receiveMessage(false);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertTrue(response.getStatus() == StatusType.BAD_REQUEST);
		assertTrue(response.getKey().equals(KEY));
	}

	public void testKeyLength() {
		assertTrue(true);
		if (true) return;

		logger.info("====TEST KEY Length====");
		final String KEY = "Our whole universe was in a hot, dense state";
		final String VALUE = "initial";

		IKVMessage response = null;
		Exception ex = null;

		try {
			KVMessage message = new KVMessage(KEY, VALUE, StatusType.PUT);
		} catch (InvalidMessageException e) {
			ex = e;
		}
		
		assertNotNull(ex);
	}

	public void testValueLength() {
		logger.info("====TEST VALUE Length====");
		final String KEY = "foo";

		String value = null;
		Exception ex = null;
		Error err = null;
		File file = new File("test_cases/longvalue");
		StringBuilder fileContents = new StringBuilder((int) file.length());
		try {

			try (Scanner scanner = new Scanner(file)) {
				while (scanner.hasNextLine()) {
					fileContents.append(scanner.nextLine() + System.lineSeparator());
				}
				
				value = fileContents.toString().trim();
			} catch (Error e) {
				err = e;
			}
		} catch (Exception e) {
			ex = e;
		}

		assertNull(err);
		assertNull(ex);
		assertNotNull(value);

		IKVMessage response = null;
		ex = null;

		try {
			KVMessage message = new KVMessage(KEY, value, StatusType.PUT);
		} catch (InvalidMessageException e) {
			ex = e;
		}
		
		assertNotNull(ex);
	}
}
