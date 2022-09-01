package testing;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collection;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import app_kvECS.ECSClient;
import app_kvECS.ECS;
import app_kvECS.IECSClient;

import ecs.IECSNode;
import ecs.ECSNode;

import client.KVStore;

import app_kvServer.KVServer;

import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;
import java.io.IOException;

public class ECSTest extends TestCase {

    private final String STORAGE_DIRECTORY = "storage/";

    private KVStore kvClient;
    private static Logger logger = Logger.getRootLogger();
    public static KVServer server;
    public static int port;

    private static ECS ecs = null;
    private Exception ex = null;

    public void setUp() {
        ex = null;
		if(ecs != null && ecs.testGetServerCount() != 0){
			try {
				logger.info("sleeping");
                ecs.removeNode();
				Thread.currentThread().sleep(2000);
			} catch (Exception e) {

				logger.info("error sleep: " + e.getMessage());
			}
		}
    }

    public void tearDown() {
        // kvClient.disconnect();
        // server.clearStorage();
    }

    public void testNoConfig() throws IOException {
        logger.info("Starting ECS test");
        try {
            ecs = new ECS(8000,2181, "");
            logger.info("Done ECS test");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ecs.testGetAvailServerCount() == 0);
    }

    public void testNormalConfig() throws IOException {
        logger.info("Starting ECS test");
        try {
            ecs = new ECS(8000,2181, "/homes/z/zulkerni/ece419/capDB/M2/ecs.config");
            logger.info("Done ECS test");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ecs instanceof ECS);
    }

    // Won't add nodes if no servers are provided in config
    public void testAddNodesWithNoServers() throws IOException {
        Collection<IECSNode> res = null;

        try {
            ecs = new ECS(8000,2181, "/homes/z/zulkerni/ece419/capDB/M2/test_cases/bad.config");
            res = ecs.addNodes(10, "FIFO", 16);
        } catch (Exception e) {
            ex = e;
        }
        for (IECSNode node : res) {
            assertNull(node);
        }
    }

    // Will now add node
    public void testAddNodeWithCorrectServer() throws IOException {
        IECSNode res = null;

        try {
            ecs = new ECS(8000,2181, "/homes/z/zulkerni/ece419/capDB/M2/test_cases/test.config");
            res = ecs.addNode("FIFO", 16);
        } catch (Exception e) {
            ex = e;
        }
        assertNotNull(res);
        assertTrue(res instanceof IECSNode);
    }

    public void testStartNode() throws IOException {
        IECSNode res = null;

        try {
            ecs = new ECS(8000,2181, "/homes/z/zulkerni/ece419/capDB/M2/test_cases/test.config");
            res = ecs.addNode("FIFO", 16);
        } catch (Exception e) {
            ex = e;
        }
        assertNotNull(res);
    }

    // public void testDeleteNode() throws IOException {
    //     IECSNode res = null;
    //     String[] path = null;

    //     logger.info("DELETE NODE test");
    //     try {
    //         ecs = new ECS(8000,2181, "/homes/z/zulkerni/ece419/capDB/M2/test_cases/test.config");
    //         ecs.addNodes(2,"FIFO", 16);
	// 		Thread.currentThread().sleep(2000);
	// 		ecs.start();
    //         KVStore kvClient = new KVStore("localhost", 50019);
    //         kvClient.connect();
    //         kvClient.put("haha", "10");
    //         IKVMessage val = kvClient.get("haha");
    //         logger.debug(val.getValue());
    //         ecs.removeNode("xman2");
	// 		// Thread.currentThread().sleep(2000);

            
    //         // File f = new File("/homes/z/zulkerni/ece419/capDB/M2/storage/");

    //     } catch (Exception e) {
    //         ex = e;
    //     }
    //     assertTrue(ecs.testGetAvailServerCount() == 1);
    // }


    // testing retrieval of file from  replicated data 
    // public void testGetReplication() throws IOException { 
    //        IECSNode res = null;
    //     IKVMessage val = null;
    //     logger.info("REPLICATION NODE test");
    //     try {
    //         ecs = new ECS(8000,2181, "/homes/z/zulkerni/ece419/capDB/M2/test_cases/test.config");
    //         ecs.addNodes(2,"FIFO", 16);
	// 		Thread.currentThread().sleep(2000);
	// 		ecs.start();
    //         KVStore kvClient = new KVStore("localhost", 50019);
    //         KVStore kvClient2 = new KVStore("localhost", 50020);
    //         kvClient.connect();
    //         kvClient.put("haha", "10");
    //         val = kvClient2.get("haha");
    //         logger.debug(val.getValue());
	// 		// Thread.currentThread().sleep(2000);            
    //         // File f = new File("/homes/z/zulkerni/ece419/capDB/M2/storage/");
    //     } catch (Exception e) {
    //         ex = e;
    //     }
    //     assertTrue(val.getValue() == "10"); 
    // }
    
    // public void testReplication() throws IOException { 
    //        IECSNode res = null;
    //     Boolean val = false;
    //     String rep = "xman2";
    //     logger.info("REPLICATION NODE test");
    //     try {
    //         ecs = new ECS(8000,2181, "/homes/z/zulkerni/ece419/capDB/M2/test_cases/test.config");
    //         ecs.addNodes(2,"FIFO", 16);
	// 		Thread.currentThread().sleep(2000);
	// 		ecs.start();
    //         KVStore kvClient = new KVStore("localhost", 50019);
    //         kvClient.connect();
    //         kvClient.put("haha", "10");
	// 		Thread.currentThread().sleep(5000);    
    //         File dir = new File("/homes/z/zulkerni/ece419/capDB/M2/storage/xman1");
    //         File[] directoryListing = dir.listFiles();
    //         for (File item : directoryListing) {
    //             try {
    //                 if (item.isDirectory()) {
    //                     String path = String.format("replica_%s/", rep);
    //                     if (item.getName().equals(path)){
    //                         File[] replicaListing = item.listFiles(); 
    //                         for (File replica : replicaListing){
    //                             if (replica.getName().equals("haha")) {
    //                                 val = true;
    //                                 break;
    //                             }
    //                         }
    //                     }
    //                     if (val) break; 
    //                 }
    //             } catch (Exception e) {
    //                 logger.error("Error while trying to replicate data");
    //                 ex = e; 
    //             }
    //         }        
    //         // File f = new File("/homes/z/zulkerni/ece419/capDB/M2/storage/");
    //     } catch (Exception e) {
    //         ex = e;
    //     }
    //     assertTrue(val); 
    // }


}
