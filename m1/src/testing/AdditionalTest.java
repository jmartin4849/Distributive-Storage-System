package testing;

import app_kvClient.KVClient;
import app_kvServer.KVServer;
import client.KVStore;
import jdk.jfr.Timestamp;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.TextMessage;

import org.apache.log4j.Level;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3

	@Test
	public void testStub() {
//		KVMessage.StatusType status = KVMessage.StatusType.GET_ERROR;
//		String key = "test_key";
//		String value = "test_value";
//		TextMessage test_message = new TextMessage(status, key, value);
//		StringBuffer result = new StringBuffer();
//		byte[] bytes = test_message.getMsgBytes();
//		for (byte b : bytes) {
//			result.append(String.format("%02X ", b));
//			result.append(" "); // delimiter
//		}
//		System.out.println(result.toString());
		assertTrue(true);
	}

	private final ByteArrayOutputStream out = new ByteArrayOutputStream();
	private final ByteArrayOutputStream err = new ByteArrayOutputStream();
	private final PrintStream originalOut = System.out;
	private final PrintStream originalErr = System.err;

	@Before
	public void setStreams() {
		System.setOut(new PrintStream(out));
		System.setErr(new PrintStream(err));
	}

	@After
	public void restoreInitialStreams() {
		System.setOut(originalOut);
		System.setErr(originalErr);
	}

	@Test
	public void test_text_message(){
		KVMessage.StatusType status = KVMessage.StatusType.GET_ERROR;
		String key = "test_value_19_bytes";
		String value = "test_value";

		byte[] test_arr = new byte[21 + value.length()];
		test_arr[0] = 0x01;
		System.arraycopy(key.getBytes(), 0, test_arr, 1, key.length());
		System.arraycopy(value.getBytes(), 0, test_arr, 21, value.length());

		TextMessage test_message = new TextMessage(test_arr);

		test_arr = TextMessage.addCtrChars(test_arr);
		assertTrue( status.equals(test_message.getStatus()) && key.equals(test_message.getKey()) && value.equals(test_message.getValue()));
	}

	/*Client tests*/
	@Test
	public void test_client_main_and_quit(){
		String data = "quit";
		InputStream testInput = new ByteArrayInputStream(data.getBytes());
		InputStream old = System.in;
		try {
			System.setIn(testInput);
			String[] args = {};
			KVClient.main(args);

		} finally {
			System.setIn(old);
		}
		assertTrue(true);
	}

	@Test
	public void test_client_disconnect(){
		try {
			new logger.LogSetup("logs/testing/client.log", Level.ALL);
		} catch (Exception e){
			System.out.println(e);
		}
		KVClient client = new KVClient();
		assertTrue(client.isRunning());
		client.handleCommand("disconnect");
		assertTrue(client.isRunning());
		assertNull(client.getStore());
	}


	@Test
	public void test_client_connect() {
		try {
			new logger.LogSetup("logs/testing/client.log", Level.ALL);
		} catch (Exception e){
			System.out.println(e);
		}
		KVServer server = new KVServer(15000, 0, null);
		KVClient client = new KVClient();
		client.handleCommand("connect 127.0.0.1 15000");
		assertEquals("127.0.0.1", client.getStore().address);
		assertEquals(15000, client.getStore().port);
		assertTrue(client.getStore().connected);
	}

	@Test
	public void test_client_get() {
		setStreams();
		try {
			new logger.LogSetup("logs/testing/client.log", Level.ALL);
		} catch (Exception e){
			System.out.println(e);
		}
		KVClient client = new KVClient();
		client.handleCommand("get");
		assertEquals("KVClient> Error! Incorrect number of parameters, pass in just one key\n", out.toString());
		out.reset();
		client.handleCommand("get test");
		assertEquals("KVClient> Error! Not connected!\n", out.toString());
		restoreInitialStreams();
	}

	@Test
	public void test_client_put() {
		setStreams();
		try {
			new logger.LogSetup("logs/testing/client.log", Level.ALL);
		} catch (Exception e){
			System.out.println(e);
		}
		KVClient client = new KVClient();
		client.handleCommand("put");
		assertEquals("KVClient> Error! Incorrect number of parameters, pass in a key followed by a value\n", out.toString());
		out.reset();
		client.handleCommand("get test");
		assertEquals("KVClient> Error! Not connected!\n", out.toString());
		restoreInitialStreams();
	}
	@Test
	public void test_server_single_store_and_get() {
		KVServer server = new KVServer(3000, 1000, "FIFO");
		server.clearStorage();
		System.out.println("Storage Cleared");
		String key = "Testing";
		String value = "123";
		try {
			server.putKV(key, value);
			assertTrue(server.inStorage(key));
			
			System.out.println("Key and value inserted");
			try {
				String out1 = server.getKV(key);
				assertEquals(out1, value);
				System.out.println("Read successful");
				System.out.println(out1);
			}
			catch(Exception e){
				assertTrue(false);
			}
		}
		catch(Exception e){
			assertTrue(false);
		}
	}

	@Test
	public void test_server_multiple_store_and_get() {
		KVServer server = new KVServer(3000, 1000, "FIFO");
		server.clearStorage();
		System.out.println("Storage Cleared");
		String key1 = "Testing";
		String value1 = "123";
		String key2 = "Testing2";
		String value2 = "456";
		String key3 = "Testing3";
		String value3 = "789";
		
		try {
			server.putKV(key1, value1);
			assertTrue(server.inStorage(key1));
			System.out.println("Key1 and value1 inserted");
			server.putKV(key2, value2);
			assertTrue(server.inStorage(key2));
			System.out.println("Key2 and value2 inserted");
			server.putKV(key3, value3);
			assertTrue(server.inStorage(key3));
			System.out.println("Key3 and value3 inserted");
			try {
				String out1 = server.getKV(key2);
				assertEquals(out1, value2);
				System.out.println("Read successful");
				System.out.println(out1);
			}
			catch(Exception e){
				assertTrue(false);
			}
		}
		catch(Exception e){
			assertTrue(false);
		}
	}
	@Test
	public void test_server_delete_store_and_get() {
		KVServer server = new KVServer(3000, 1000, "FIFO");
		server.clearStorage();
		System.out.println("Storage Cleared");
		String key1 = "Testing";
		String value1 = "123";
		String key2 = "Testing2";
		String value2 = "456";
		
		try {
			server.putKV(key1, value1);
			assertTrue(server.inStorage(key1));
			System.out.println("Key1 and value1 inserted");
			server.putKV(key2, value2);
			assertTrue(server.inStorage(key2));
			System.out.println("Key2 and value2 inserted");
			try {
				server.putKV(key2, "");
				assertFalse(server.inStorage(key2));
				System.out.println("delete successful");
			}
			catch(Exception e){
				assertTrue(false);
			}
		}
		catch(Exception e){
			assertTrue(false);
		}
	}
	
	@Test
	public void test_server_clear() {
		KVServer server = new KVServer(3000, 1000, "FIFO");
		server.clearStorage();
		System.out.println("Storage Cleared");
		String key1 = "Testing";
		String value1 = "123";
		String key2 = "Testing2";
		String value2 = "456";
		String key3 = "Testing3";
		String value3 = "789";
		try {
			server.putKV(key1, value1);
			assertTrue(server.inStorage(key1));
			System.out.println("Key1 and value1 inserted");
			server.putKV(key2, value2);
			assertTrue(server.inStorage(key2));
			System.out.println("Key2 and value2 inserted");
			server.putKV(key3, value3);
			assertTrue(server.inStorage(key3));
			System.out.println("Key3 and value3 inserted");
			server.clearStorage();
			
			try {
				assertFalse(server.inStorage(key1));
				assertFalse(server.inStorage(key2));
				assertFalse(server.inStorage(key3));
			}
			catch(Exception e){
				assertTrue(false);
			}
		}
		catch(Exception e){
			assertTrue(false);
		}
	}

	@Test
	public void test_server_update() {
		KVServer server = new KVServer(3000, 1000, "FIFO");
		server.clearStorage();
		System.out.println("Storage Cleared");
		String key = "Testing";
		String value = "123";
		String value2 = "456";
		
		try {
			server.putKV(key, value);
			assertEquals(server.getKV(key), value);
			server.putKV(key, value2);
			assertEquals(server.getKV(key), value2);
		}
		catch(Exception e){
			assertTrue(false);
		}
	}


}
