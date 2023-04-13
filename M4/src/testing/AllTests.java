package testing;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;

import app_kvECS.ECS;
import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	public static void clearStorage(String path) {
		File directory = new File(path);
		boolean deleted = true;
		for (File f : directory.listFiles()) {
			if (f.isDirectory()) {
				System.out.println("Ignoring directory");
			} else {
				System.out.println("Deleting " + f.getName());
				deleted = deleted && f.delete();
				System.out.println(deleted);
			}
		}
//		return deleted;
	}

	static {

		Thread thread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					new LogSetup("logs/testing/test.log", Level.ERROR);
					new ECS(8001, InetAddress.getByName("127.0.0.1")).run();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		Thread thread2 = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					new LogSetup("logs/testing/test.log", Level.ERROR);
					//TODO: Broken test
					new KVServer(50000, 10, "FIFO", "sample_keys",
							InetAddress.getByName("127.0.0.1"), InetAddress.getByName("127.0.0.1"), 8001).run();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		thread.start();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		thread2.start();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		System.out.println("CLEARING");
		AllTests.clearStorage("sample_keys");
		AllTests.clearStorage("sample_keys2");
		AllTests.clearStorage("sample_keys3");
	}


	public static Test suite() throws IOException {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class);
		clientSuite.addTestSuite(AdditionalTest.class);


		return clientSuite;
	}
}
