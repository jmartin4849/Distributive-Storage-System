package testing;
import org.json.JSONObject;
import app_kvClient.KVClient;
import app_kvServer.KVServer;
import client.KVStore;
import jdk.jfr.Timestamp;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

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

	private KVStore kv_store;
	private KVServer kv_server;

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

	@Test
	public void test_file_creation_by_server_name(){
		String testname = "testing.json";
		assertTrue(KVServer.createStorage(testname));
		try (FileReader reader = new FileReader(testname)){
		} catch (FileNotFoundException e){
			assertTrue(false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test_file_creation_invalid_input(){
		boolean notFound = false;
		String testname = "testing.doc.json";
		assertFalse(KVServer.createStorage(testname));
		try (FileReader reader = new FileReader(testname)){

		}catch (FileNotFoundException e){
			notFound = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertTrue(notFound);
	}
	@Test
	public void test_server_clear(){
		String f = "TestingClear.json";
		JSONObject storage = new JSONObject();
		storage.put("testing", "123");
		try (FileWriter writer = new FileWriter(f, false)){
			writer.write(storage.toString());
			writer.flush();
		} catch (Exception e){
			assertTrue(false);
		}
		assertTrue(KVServer.clearStorageFile(f));
	}

	@Test
	public void test_server_clear_file_doesnt_exist(){
		String f = "123filedoesn'texist.json";
		assertTrue(KVServer.clearStorageFile(f));
	}
	/*Client tests*/
	//Test file creation by server name
	//Test multiple servers running concurrently
	//Testing if server is stopped on instantion

	@Test
	public void test_store_teardown_connection() {
		kv_store = new KVStore("localhost", 50010);
		try{
			kv_store.tearDownConnection();
		} catch (IOException e){

		}
		assertFalse( kv_store.connected);
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
	public void test_stored_key() {
		String stored_key;
		kv_store = new KVStore("localhost", 5001);
		String key = "thisismykey";
		stored_key = (String)kv_store.get_stored_key(key);
		assertFalse(stored_key == null);
	}



}
