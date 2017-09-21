package gov.usdot.cv.subscription.datasink;

import static org.junit.Assert.assertTrue;
import gov.usdot.asn1.generated.j2735.J2735;
import gov.usdot.asn1.generated.j2735.semi.DataSubscriptionResponse;
import gov.usdot.asn1.generated.j2735.semi.SemiDialogID;
import gov.usdot.asn1.j2735.J2735Util;
import gov.usdot.cv.common.subscription.response.ResponseCode;
import gov.usdot.cv.common.util.UnitTestHelper;
import gov.usdot.cv.resources.PrivateTestResourceLoader;
import gov.usdot.cv.subscription.datasink.receiver.SubscriptionResponseReceiver;
import gov.usdot.cv.subscription.datasink.security.CertificateUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import net.sf.json.JSONObject;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.deleidos.rtws.commons.config.RtwsConfig;
import com.deleidos.rtws.commons.dao.jdbc.DataAccessSession;
import com.deleidos.rtws.commons.dao.source.H2ConnectionPool;
import com.deleidos.rtws.commons.dao.type.sql.SqlTypeHandler;
import com.oss.asn1.AbstractData;
import com.oss.asn1.Coder;

@SuppressWarnings("deprecation")
public class SubscriptionProcessorTest {
	
	static final private boolean isDebugOutput = false;
	
	static
	{
		UnitTestHelper.initLog4j(isDebugOutput);
		Properties testProperties = System.getProperties();
		if (testProperties.getProperty("RTWS_CONFIG_DIR") == null) {
			testProperties.setProperty("RTWS_CONFIG_DIR", testProperties.getProperty("basedir", "."));
			try {
				testProperties.load(
						PrivateTestResourceLoader.getFileAsStream(
								"@properties/datasink-subscription-filtering.properties@"));

				System.setProperties(testProperties);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	// valid subscription requests
	private static final JSONObject tmc_req1 = JSONObject.fromObject("{\"dialogId\":155,\"sequenceId\":8,\"endTime\":\"3020-02-28T10:10:00\",\"destHost\":\"127.0.0.1\",\"destPort\":7443,\"fromForwarder\":\"false\",\"type\":\"VsmType\",\"typeValue\":1,\"requestId\":1001}");
	private static final JSONObject tmc_req2 = JSONObject.fromObject("{\"dialogId\":155,\"sequenceId\":8,\"endTime\":\"3020-02-28T10:10:00\",\"destHost\":\"127.0.0.1\",\"destPort\":7443,\"fromForwarder\":\"false\",\"type\":\"VsmType\",\"typeValue\":1,\"requestId\":1001}");
	private static final JSONObject pc_req1  = JSONObject.fromObject("{\"dialogId\":155,\"sequenceId\":8,\"endTime\":\"3020-02-28T10:10:00\",\"destHost\":\"localhost\",\"destPort\":7443,\"fromForwarder\":\"false\",\"type\":\"VsmType\",\"typeValue\":1,\"requestId\":1001,\"nwPos\":{\"lat\":43.0,\"lon\":-85.0},\"sePos\":{\"lat\":41.0,\"lon\":-82.0}}");
	
	// cancellation requests
	private static final JSONObject tmc_req1_cancel = JSONObject.fromObject("{\"dialogId\":155,\"sequenceId\":10,\"requestId\":1001}");
	private static final JSONObject tmc_req2_cancel = JSONObject.fromObject("{\"dialogId\":155,\"sequenceId\":10,\"requestId\":9999}");
	
	// missing end time property
	private static final JSONObject missingprop_req1 = JSONObject.fromObject("{\"dialogId\":155,\"sequenceId\":8,\"destHost\":\"127.0.0.1\",\"destPort\":7443,\"fromForwarder\":\"false\",\"type\":\"VsmType\",\"typeValue\":1,\"requestId\":1001}");
	// missing target port property
	private static final JSONObject missingprop_req2 = JSONObject.fromObject("{\"dialogId\":155,\"sequenceId\":8,\"destHost\":\"127.0.0.1\",\"fromForwarder\":\"false\",\"endTime\":\"3020-02-28T10:10:00Z\",\"type\":\"VsmType\",\"typeValue\":1,\"requestId\":1001}");
	// missing nw lon property
	private static final JSONObject missingprop_req3 = JSONObject.fromObject("{\"dialogId\":155,\"sequenceId\":8,\"destHost\":\"127.0.0.1\",\"destPort\":7443,\"fromForwarder\":\"false\",\"endTime\":\"3020-02-28T10:10:00Z\",\"type\":\"VsmType\",\"typeValue\":1,\"requestId\":1001,\"nwPos\":{\"lat\":43.0},\"sePos\":{\"lat\":41.0,\"lon\":-82.0}}");
	// end time is before the current time
	private static final JSONObject badtime_req1 = JSONObject.fromObject("{\"dialogId\":155,\"sequenceId\":8,\"destHost\":\"127.0.0.1\",\"destPort\":7443,\"fromForwarder\":\"false\",\"endTime\":\"2012-01-31T10:10:00\",\"type\":\"VsmType\",\"typeValue\":1,\"requestId\":1001}");
	// nw lat is greater than 90
	private static final JSONObject badbox1 = JSONObject.fromObject("{\"dialogId\":155,\"sequenceId\":8,\"destHost\":\"127.0.0.1\",\"destPort\":7443,\"fromForwarder\":\"false\",\"endTime\":\"3020-02-28T10:10:00Z\",\"type\":\"VsmType\",\"typeValue\":1,\"requestId\":1001,\"nwPos\":{\"lat\":125.0,\"lon\":-85.0},\"sePos\":{\"lat\":41.0,\"lon\":-82.0}}");
	// nw position is below se position 
	private static final JSONObject badbox2 = JSONObject.fromObject("{\"dialogId\":155,\"sequenceId\":8,\"destHost\":\"127.0.0.1\",\"destPort\":7443,\"fromForwarder\":\"false\",\"endTime\":\"3020-02-28T10:10:00Z\",\"type\":\"VsmType\",\"typeValue\":1,\"requestId\":1001,\"nwPos\":{\"lat\":-50.0,\"lon\":-85.0},\"sePos\":{\"lat\":41.0,\"lon\":-82.0}}");
	// nw/se box is bigger than the supported region
	private static final JSONObject badbox3 = JSONObject.fromObject("{\"dialogId\":155,\"sequenceId\":8,\"destHost\":\"127.0.0.1\",\"destPort\":7443,\"fromForwarder\":\"false\",\"endTime\":\"3020-02-28T10:10:00Z\",\"type\":\"VsmType\",\"typeValue\":1,\"requestId\":1001,\"nwPos\":{\"lat\":45.0,\"lon\":-85.0},\"sePos\":{\"lat\":41.0,\"lon\":-85.0}}");
	
	private static final String jdbcUrl = "jdbc:h2:%s";
	private static DataSource dataSource;
	private static DataAccessSession session;
	
	private static Coder coder;
	
	private static ArrayBlockingQueue<ByteBuffer> queue = new ArrayBlockingQueue<ByteBuffer>(25);
	private static Thread receiver_t;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		System.out.println("Configuring subscription database ...");
		
		StringBuilder dirPath = new StringBuilder();
		dirPath.append(System.getProperty("user.dir")).append(File.separator).append("src").append(File.separator);
		dirPath.append("test").append(File.separator).append("resources").append(File.separator);
		dirPath.append("db").append(File.separator).append("testdb");
		
		String url = String.format(jdbcUrl, dirPath.toString());
		System.out.println("Setting unit test database directory at '" + dirPath + "'.");
		
		String username = RtwsConfig.getInstance().getString("h2.sa.connection.user");
		String password = RtwsConfig.getInstance().getString("h2.sa.connection.password");
		
		System.out.println("Starting subscription database ...");
		DriverManager.getConnection(url, username, password);
		
		dataSource = new H2ConnectionPool();
		((H2ConnectionPool) dataSource).setURL(url);
		((H2ConnectionPool) dataSource).setUser(username);
		((H2ConnectionPool) dataSource).setPassword(password);
		session = DataAccessSession.session(dataSource);
		
		J2735.initialize();
		coder = J2735.getPERUnalignedCoder();
		
		receiver_t = new Thread(new SubscriptionResponseReceiver(queue, 7443));
		receiver_t.start();
	}
	
	@Before
	public void setUp() throws Exception {
		System.out.println("Creating APPLICATION schema ... ");
		session.executeStatement("CREATE SCHEMA IF NOT EXISTS APPLICATION;", null);
		
		System.out.println("Removing database entries ...");
		session.executeStatement("DROP TABLE IF EXISTS APPLICATION.TEST_SITUATION_DATA_FILTER;", null);
		session.executeStatement("DROP TABLE IF EXISTS APPLICATION.TEST_SUBSCRIBER;", null);
		
		System.out.println("Generating public key ...");
		String publicKey = CertificateUtil.generatePublicKey();
		
		System.out.println("Adding public key to subscription requests ...");
		tmc_req1.put("certificate", publicKey);
		tmc_req2.put("certificate", publicKey);
		pc_req1.put("certificate", publicKey);
		badtime_req1.put("certificate", publicKey);
		missingprop_req1.put("certificate", publicKey);
		missingprop_req2.put("certificate", publicKey);
		missingprop_req3.put("certificate", publicKey);
		badbox1.put("certificate", publicKey);
		badbox2.put("certificate", publicKey);
		badbox3.put("certificate", publicKey);
		
		System.out.println("Setting 'RTWS_FQDN' and 'RTWS_DOMAIN' properties ...");
		System.setProperty("RTWS_FQDN", "cv-subscription1.dev-lcsdw-subscription.rtsaic.com");
		System.setProperty("RTWS_DOMAIN", "dev-lcsdw-subscription.rtsaic.com");
	}
	
	@Test 
	public void testSubscriptionTablesExist() throws Exception {
		System.out.println(">>> Running testSubscriptionTablesExist() ...");
		
		SubscriptionProcessor processor = new SubscriptionProcessor();
		processor.setDataSource(dataSource);
		processor.setDatabaseSubscriberTableName("TEST_SUBSCRIBER");
		processor.setDatabaseFilterTableName("TEST_SITUATION_DATA_FILTER");
		processor.setNorthwestLatitude(43.0);
		processor.setNorthwestLongitude(-85.0);
		processor.setSoutheastLatitude(41.0);
		processor.setSoutheastLongitude(82.0);
		processor.initialize();
		processor.dispose();
		
		// verify the two tables are created
		String stmt = "SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='TEST_SUBSCRIBER' OR TABLE_NAME='TEST_SITUATION_DATA_FILTER';";
		int count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);

		Assert.assertTrue("Failed to find subscriber and situation data filter tables.", count == 2);
	}
	
	@Test
	public void testStoreSubscriptionRequest() throws Exception {
		System.out.println(">>> Running testStoreSubscriptionRequest() ...");
		
		SubscriptionProcessor processor = new SubscriptionProcessor();
		processor.setDataSource(dataSource);
		processor.setDatabaseSubscriberTableName("TEST_SUBSCRIBER");
		processor.setDatabaseFilterTableName("TEST_SITUATION_DATA_FILTER");
		processor.setNorthwestLatitude(43.0);
		processor.setNorthwestLongitude(-85.0);
		processor.setSoutheastLatitude(41.0);
		processor.setSoutheastLongitude(82.0);
		processor.initialize();
		processor.process(tmc_req1);
		processor.process(tmc_req2);
		processor.process(pc_req1);
		processor.dispose();
		
		// verify that the subscription request was added to the database
		String stmt = "SELECT count(*) FROM APPLICATION.TEST_SUBSCRIBER WHERE ID=10000000;";
		int count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		assertTrue("Failed to find subscriber id '10000000' in subscriber table.", count == 1);
		
		stmt = "SELECT count(*) FROM APPLICATION.TEST_SITUATION_DATA_FILTER WHERE ID=10000000;";
		count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		assertTrue("Failed to find subscriber id '10000000' in situation data filter table.", count == 1);
		
		stmt = "SELECT count(*) FROM APPLICATION.TEST_SUBSCRIBER WHERE ID=10000001;";
		count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		assertTrue("Failed to find subscriber id '10000001' in subscriber table.", count == 1);
		
		stmt = "SELECT count(*) FROM APPLICATION.TEST_SITUATION_DATA_FILTER WHERE ID=10000001;";
		count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		assertTrue("Failed to find subscriber id '10000001' in situation data filter table.", count == 1);
		
		stmt = "SELECT count(*) FROM APPLICATION.TEST_SUBSCRIBER WHERE ID=10000002;";
		count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		assertTrue("Failed to find subscriber id '10000002' in subscriber table.", count == 1);
		
		stmt = "SELECT count(*) FROM APPLICATION.TEST_SITUATION_DATA_FILTER WHERE ID=10000002;";
		count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		assertTrue("Failed to find subscriber id '10000002' in situation data filter table.", count == 1);
	
		List<ByteBuffer> responses = collectResponses(3);
		assertTrue("Expecting 3 responses but got '" + responses.size() + "'.", responses.size() == 3);
		
		AbstractData message = J2735Util.decode(coder, responses.get(0).array());
		assertTrue("Expecting a message of type 'DPCSubscriptionResponse'.", message instanceof DataSubscriptionResponse);
		assertTrue("Expecting the SemiDialogID to be 'lcsdwDataDist'.", ((DataSubscriptionResponse) message).getDialogID() == SemiDialogID.dataSubscription);
		assertTrue("Expecting the subscriber id to be '10000000'.", ByteBuffer.wrap(((DataSubscriptionResponse) message).getSubID().byteArrayValue()).getInt() == 10000000);

		message = J2735Util.decode(coder, responses.get(1).array());
		assertTrue("Expecting a message of type 'DPCSubscriptionResponse'.", message instanceof DataSubscriptionResponse);
		assertTrue("Expecting the SemiDialogID to be 'lcsdwDataDist'.", ((DataSubscriptionResponse) message).getDialogID() == SemiDialogID.dataSubscription);
		assertTrue("Expecting the subscriber id to be '10000001'.", ByteBuffer.wrap(((DataSubscriptionResponse) message).getSubID().byteArrayValue()).getInt() == 10000001);
		
		message = J2735Util.decode(coder, responses.get(2).array());
		assertTrue("Expecting a message of type 'DPCSubscriptionResponse'.", message instanceof DataSubscriptionResponse);
		assertTrue("Expecting the SemiDialogID to be 'lcsdwDataDist'.", ((DataSubscriptionResponse) message).getDialogID() == SemiDialogID.dataSubscription);
		assertTrue("Expecting the subscriber id to be '10000002'.", ByteBuffer.wrap(((DataSubscriptionResponse) message).getSubID().byteArrayValue()).getInt() == 10000002);
	}
	
	@Test @org.junit.Ignore
	public void testMissingProperties() throws Exception {
		System.out.println(">>> Running testMissingProperties() ...");
		
		SubscriptionProcessor processor = new SubscriptionProcessor();
		processor.setDataSource(dataSource);
		processor.setDatabaseSubscriberTableName("TEST_SUBSCRIBER");
		processor.setDatabaseFilterTableName("TEST_SITUATION_DATA_FILTER");
		processor.setNorthwestLatitude(43.0);
		processor.setNorthwestLongitude(-85.0);
		processor.setSoutheastLatitude(41.0);
		processor.setSoutheastLongitude(82.0);
		processor.initialize();
		processor.process(missingprop_req1);
		processor.process(missingprop_req2); // Not expecting a response because target port is missing
		processor.process(missingprop_req3);
		processor.dispose();
		
		// verify that the subscription request was added to the database
		String stmt = "SELECT count(*) FROM APPLICATION.TEST_SUBSCRIBER;";
		int count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		Assert.assertTrue("No subscribers should be found because properties are missing.", count == 0);
		
		List<ByteBuffer> responses = collectResponses(3);
		assertTrue("Expecting 2 responses but got '" + responses.size() + "'.", responses.size() == 2);
		
		AbstractData message = J2735Util.decode(coder, responses.get(0).array());
		assertTrue("Expecting a message of type 'DPCSubscriptionResponse'.", message instanceof DataSubscriptionResponse);
		assertTrue("Expecting the SemiDialogID to be 'lcsdwDataDist'.", ((DataSubscriptionResponse) message).getDialogID() == SemiDialogID.dataSubscription);
		assertTrue("Expecting the subscriber id to be '0'.", ByteBuffer.wrap(((DataSubscriptionResponse) message).getSubID().byteArrayValue()).getInt() == 0);
		assertTrue("Expecting the error code (missing end time) to be '7'.", ((DataSubscriptionResponse) message).getErr() == 7);
		
		message = J2735Util.decode(coder, responses.get(1).array());
		assertTrue("Expecting a message of type 'DPCSubscriptionResponse'.", message instanceof DataSubscriptionResponse);
		assertTrue("Expecting the SemiDialogID to be 'lcsdwDataDist'.", ((DataSubscriptionResponse) message).getDialogID() == SemiDialogID.dataSubscription);
		assertTrue("Expecting the subscriber id to be '0'.", ByteBuffer.wrap(((DataSubscriptionResponse) message).getSubID().byteArrayValue()).getInt() == 0);
		assertTrue("Expecting the error code (missing nw lon) to be '13'.", ((DataSubscriptionResponse) message).getErr() == ResponseCode.NWLonMissing.getCode());
	}
	
	@Test @org.junit.Ignore
	public void testInvalidEndTime() throws Exception {
		System.out.println(">>> Running testInvalidEndTime() ...");
		
		SubscriptionProcessor processor = new SubscriptionProcessor();
		processor.setDataSource(dataSource);
		processor.setDatabaseSubscriberTableName("TEST_SUBSCRIBER");
		processor.setDatabaseFilterTableName("TEST_SITUATION_DATA_FILTER");
		processor.setNorthwestLatitude(43.0);
		processor.setNorthwestLongitude(-85.0);
		processor.setSoutheastLatitude(41.0);
		processor.setSoutheastLongitude(82.0);
		processor.initialize();
		processor.process(badtime_req1);
		processor.dispose();
		
		// verify that the subscription request was added to the database
		String stmt = "SELECT count(*) FROM APPLICATION.TEST_SUBSCRIBER;";
		int count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		Assert.assertTrue("No subscribers should be found because end time is invalid.", count == 0);
		
		List<ByteBuffer> responses = collectResponses(3);
		assertTrue("Expecting 1 response but got '" + responses.size() + "'.", responses.size() == 1);
		
		AbstractData message = J2735Util.decode(coder, responses.get(0).array());
		assertTrue("Expecting a message of type 'DPCSubscriptionResponse'.", message instanceof DataSubscriptionResponse);
		assertTrue("Expecting the SemiDialogID to be 'lcsdwDataDist'.", ((DataSubscriptionResponse) message).getDialogID() == SemiDialogID.dataSubscription);
		assertTrue("Expecting the subscriber id to be '0'.", ByteBuffer.wrap(((DataSubscriptionResponse) message).getSubID().byteArrayValue()).getInt() == 0);
		assertTrue("Expecting the error code (invalid end time) to be '104'.", ((DataSubscriptionResponse) message).getErr() == 104);
	}
	
	@Test
	public void testBadBoundingBox() throws Exception {
		System.out.println(">>> Running testBadBoundingBox() ...");
		
		SubscriptionProcessor processor = new SubscriptionProcessor();
		processor.setDataSource(dataSource);
		processor.setDatabaseSubscriberTableName("TEST_SUBSCRIBER");
		processor.setDatabaseFilterTableName("TEST_SITUATION_DATA_FILTER");
		processor.setNorthwestLatitude(43.0);
		processor.setNorthwestLongitude(-85.0);
		processor.setSoutheastLatitude(41.0);
		processor.setSoutheastLongitude(82.0);
		processor.initialize();
		processor.process(badbox1);
		processor.process(badbox2);
		processor.process(badbox3);
		processor.dispose();
		
		// verify that the subscription request was added to the database
		String stmt = "SELECT count(*) FROM APPLICATION.TEST_SUBSCRIBER;";
		int count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		Assert.assertTrue("No subscribers should be found because bounding box is invalid.", count == 0);
		
		List<ByteBuffer> responses = collectResponses(3);
		assertTrue("Expecting 3 responses but got '" + responses.size() + "'.", responses.size() == 3);
		
		AbstractData message = J2735Util.decode(coder, responses.get(0).array());
		assertTrue("Expecting a message of type 'DPCSubscriptionResponse'.", message instanceof DataSubscriptionResponse);
		assertTrue("Expecting the SemiDialogID to be 'lcsdwDataDist'.", ((DataSubscriptionResponse) message).getDialogID() == SemiDialogID.dataSubscription);
		assertTrue("Expecting the subscriber id to be '0'.", ByteBuffer.wrap(((DataSubscriptionResponse) message).getSubID().byteArrayValue()).getInt() == 0);
		assertTrue("Expecting the error code (invalid bounding box) to be '106'.", ((DataSubscriptionResponse) message).getErr() == 106);
		
		message = J2735Util.decode(coder, responses.get(1).array());
		assertTrue("Expecting a message of type 'DPCSubscriptionResponse'.", message instanceof DataSubscriptionResponse);
		assertTrue("Expecting the SemiDialogID to be 'lcsdwDataDist'.", ((DataSubscriptionResponse) message).getDialogID() == SemiDialogID.dataSubscription);
		assertTrue("Expecting the subscriber id to be '0'.", ByteBuffer.wrap(((DataSubscriptionResponse) message).getSubID().byteArrayValue()).getInt() == 0);
		assertTrue("Expecting the error code (invalid bounding box) to be '106'.", ((DataSubscriptionResponse) message).getErr() == 106);
		
		message = J2735Util.decode(coder, responses.get(2).array());
		assertTrue("Expecting a message of type 'DPCSubscriptionResponse'.", message instanceof DataSubscriptionResponse);
		assertTrue("Expecting the SemiDialogID to be 'lcsdwDataDist'.", ((DataSubscriptionResponse) message).getDialogID() == SemiDialogID.dataSubscription);
		assertTrue("Expecting the subscriber id to be '0'.", ByteBuffer.wrap(((DataSubscriptionResponse) message).getSubID().byteArrayValue()).getInt() == 0);
		assertTrue("Expecting the error code (invalid bounding box) to be '106'.", ((DataSubscriptionResponse) message).getErr() == 106);
	}
	
	@Test
	public void testCancelSubcriptionRequest() throws Exception {
		System.out.println(">>> Running testCancelSubcriptionRequest() ...");
		
		SubscriptionProcessor processor = new SubscriptionProcessor();
		processor.setDataSource(dataSource);
		processor.setDatabaseSubscriberTableName("TEST_SUBSCRIBER");
		processor.setDatabaseFilterTableName("TEST_SITUATION_DATA_FILTER");
		processor.setNorthwestLatitude(43.0);
		processor.setNorthwestLongitude(-85.0);
		processor.setSoutheastLatitude(41.0);
		processor.setSoutheastLongitude(82.0);
		processor.initialize();
		processor.process(tmc_req1);
		
		List<ByteBuffer> responses = collectResponses(1);
		assertTrue("Expecting 1 responses but got '" + responses.size() + "'.", responses.size() == 1);
		
		AbstractData message = J2735Util.decode(coder, responses.get(0).array());
		assertTrue("Expecting a message of type 'DPCSubscriptionResponse'.", message instanceof DataSubscriptionResponse);
		assertTrue("Expecting the SemiDialogID to be 'lcsdwDataDist'.", ((DataSubscriptionResponse) message).getDialogID() == SemiDialogID.dataSubscription);
		
		int subscriberId = ByteBuffer.wrap(((DataSubscriptionResponse) message).getSubID().byteArrayValue()).getInt();
		
		// verify that the subscription request was added to the database
		String stmt = "SELECT count(*) FROM APPLICATION.TEST_SUBSCRIBER WHERE ID=" + subscriberId + ";";
		int count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		assertTrue("Failed to find subscriber id '" + subscriberId + "' in subscriber table.", count == 1);
		
		stmt = "SELECT count(*) FROM APPLICATION.TEST_SITUATION_DATA_FILTER WHERE ID=" + subscriberId + " AND REQUEST_ID=1001;";
		count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		assertTrue("Failed to find subscriber id '" + subscriberId + "' in situation data filter table.", count == 1);

		tmc_req1_cancel.put("subscriberId", subscriberId);
		processor.process(tmc_req1_cancel);
		
		// verify the subscription request was removed from the database
		stmt = "SELECT count(*) FROM APPLICATION.TEST_SUBSCRIBER WHERE ID=" + subscriberId + ";";
		count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		assertTrue("Found subscriber id '" + subscriberId + "' in subscriber table, should not exist.", count == 0);
		
		stmt = "SELECT count(*) FROM APPLICATION.TEST_SITUATION_DATA_FILTER WHERE ID=" + subscriberId + " AND REQUEST_ID=1001;";
		count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		assertTrue("Found subscriber id '" + subscriberId + "' and request id '1001' in situation data filter table, should not exist.", count == 0);
		
		processor.dispose();
		
		responses = collectResponses(1);
		assertTrue("Expecting 1 responses but got '" + responses.size() + "'.", responses.size() == 1);
		
		message = J2735Util.decode(coder, responses.get(0).array());
		assertTrue("Expecting a message of type 'DPCSubscriptionResponse'.", message instanceof DataSubscriptionResponse);
		assertTrue("Expecting the SemiDialogID to be 'lcsdwDataDist'.", ((DataSubscriptionResponse) message).getDialogID() == SemiDialogID.dataSubscription);
		assertTrue("Expecting the subscriber id to be '" + subscriberId + "'.", ByteBuffer.wrap(((DataSubscriptionResponse) message).getSubID().byteArrayValue()).getInt() == subscriberId);
	}
	
	@Test
	public void testInvalidCancelSubcriptionRequest() throws Exception {
		System.out.println(">>> Running testInvalidCancelSubcriptionRequest() ...");
		
		SubscriptionProcessor processor = new SubscriptionProcessor();
		processor.setDataSource(dataSource);
		processor.setDatabaseSubscriberTableName("TEST_SUBSCRIBER");
		processor.setDatabaseFilterTableName("TEST_SITUATION_DATA_FILTER");
		processor.setNorthwestLatitude(43.0);
		processor.setNorthwestLongitude(-85.0);
		processor.setSoutheastLatitude(41.0);
		processor.setSoutheastLongitude(82.0);
		processor.initialize();
		processor.process(tmc_req2);
		
		List<ByteBuffer> responses = collectResponses(1);
		assertTrue("Expecting 1 responses but got '" + responses.size() + "'.", responses.size() == 1);
		
		AbstractData message = J2735Util.decode(coder, responses.get(0).array());
		assertTrue("Expecting a message of type 'DPCSubscriptionResponse'.", message instanceof DataSubscriptionResponse);
		assertTrue("Expecting the SemiDialogID to be 'lcsdwDataDist'.", ((DataSubscriptionResponse) message).getDialogID() == SemiDialogID.dataSubscription);
		
		int subscriberId = ByteBuffer.wrap(((DataSubscriptionResponse) message).getSubID().byteArrayValue()).getInt();
		
		// verify that the subscription request was added to the database
		String stmt = "SELECT count(*) FROM APPLICATION.TEST_SUBSCRIBER WHERE ID=" + subscriberId + ";";
		int count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		assertTrue("Failed to find subscriber id '" + subscriberId + "' in subscriber table.", count == 1);
		
		stmt = "SELECT count(*) FROM APPLICATION.TEST_SITUATION_DATA_FILTER WHERE ID=" + subscriberId + " AND REQUEST_ID=1001;";
		count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		assertTrue("Failed to find subscriber id '" + subscriberId + "' in situation data filter table.", count == 1);

		tmc_req2_cancel.put("subscriberId", subscriberId);
		processor.process(tmc_req2_cancel);
		
		// verify the subscription request still exist in the database
		stmt = "SELECT count(*) FROM APPLICATION.TEST_SUBSCRIBER WHERE ID=" + subscriberId + ";";
		count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		assertTrue("Failed to find subscriber id '" + subscriberId + "' in subscriber table.", count == 1);
		
		stmt = "SELECT count(*) FROM APPLICATION.TEST_SITUATION_DATA_FILTER WHERE ID=" + subscriberId + " AND REQUEST_ID=1001;";
		count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		assertTrue("Failed to find subscriber id '" + subscriberId + "' and request id '1001' in situation data filter table.", count == 1);
		
		processor.dispose();
		
		responses = collectResponses(1);
		assertTrue("Expecting 1 responses but got '" + responses.size() + "'.", responses.size() == 1);
		
		message = J2735Util.decode(coder, responses.get(0).array());
		assertTrue("Expecting a message of type 'DPCSubscriptionResponse'.", message instanceof DataSubscriptionResponse);
		assertTrue("Expecting the SemiDialogID to be 'lcsdwDataDist'.", ((DataSubscriptionResponse) message).getDialogID() == SemiDialogID.dataSubscription);
		assertTrue("Expecting the subscriber id to be '" + subscriberId + "'.", ByteBuffer.wrap(((DataSubscriptionResponse) message).getSubID().byteArrayValue()).getInt() == subscriberId);
		assertTrue("Expecting the error code (invalid request id) to be '105'.", ((DataSubscriptionResponse) message).getErr() == 105);
	}
	
	@Test
	public void testSubscriptionExpired() throws Exception {
		System.out.println(">>> Running testSubscriptionExpired() ...");

		System.setProperty("subscription.expiration.processor.interval", "100");
		
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"); 
		Calendar current = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		current.setTimeInMillis(System.currentTimeMillis() + 5000);
		System.out.println("Setting filter expiration to '" + df.format(current.getTime()) + "'.");
		
		tmc_req1.put("endTime", df.format(current.getTime()));
		pc_req1.put("endTime", df.format(current.getTime()));
		
		SubscriptionProcessor processor = new SubscriptionProcessor();
		processor.setDataSource(dataSource);
		processor.setDatabaseSubscriberTableName("TEST_SUBSCRIBER");
		processor.setDatabaseFilterTableName("TEST_SITUATION_DATA_FILTER");
		processor.setNorthwestLatitude(43.0);
		processor.setNorthwestLongitude(-85.0);
		processor.setSoutheastLatitude(41.0);
		processor.setSoutheastLongitude(82.0);
		processor.initialize();
		processor.process(tmc_req1);
		processor.process(pc_req1);

		try { Thread.sleep(15 * 1000); } catch (InterruptedException ignore) {}
		
		List<ByteBuffer> responses = collectResponses(2);
		assertTrue("Expecting 2 responses but got '" + responses.size() + "'.", responses.size() == 2);
		
		AbstractData message = J2735Util.decode(coder, responses.get(0).array());
		assertTrue("Expecting a message of type 'DPCSubscriptionResponse'.", message instanceof DataSubscriptionResponse);
		assertTrue("Expecting the SemiDialogID to be 'lcsdwDataDist'.", ((DataSubscriptionResponse) message).getDialogID() == SemiDialogID.dataSubscription);
		assertTrue("Expecting the subscriber id to not be '0'.", ByteBuffer.wrap(((DataSubscriptionResponse) message).getSubID().byteArrayValue()).getInt() != 0);
		int tmc_subid = ByteBuffer.wrap(((DataSubscriptionResponse) message).getSubID().byteArrayValue()).getInt();
		
		message = J2735Util.decode(coder, responses.get(1).array());
		assertTrue("Expecting a message of type 'DPCSubscriptionResponse'.", message instanceof DataSubscriptionResponse);
		assertTrue("Expecting the SemiDialogID to be 'lcsdwDataDist'.", ((DataSubscriptionResponse) message).getDialogID() == SemiDialogID.dataSubscription);
		assertTrue("Expecting the subscriber id to not be '0'.", ByteBuffer.wrap(((DataSubscriptionResponse) message).getSubID().byteArrayValue()).getInt() != 0);
		int pc_subid = ByteBuffer.wrap(((DataSubscriptionResponse) message).getSubID().byteArrayValue()).getInt();
		
		String stmt = "SELECT count(*) FROM APPLICATION.TEST_SUBSCRIBER WHERE ID=" + tmc_subid + ";";
		int count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		assertTrue("Subscriber id '" + tmc_subid + "' should be removed from the subscriber table.", count == 0);
		
		stmt = "SELECT count(*) FROM APPLICATION.TEST_SITUATION_DATA_FILTER WHERE ID=" + tmc_subid + " AND REQUEST_ID=1001;";
		count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		assertTrue("Subscriber id '" + tmc_subid + "' and request id '1001' should be removed from the situation data filter table.", count == 0);
		
		stmt = "SELECT count(*) FROM APPLICATION.TEST_SUBSCRIBER WHERE ID=" + pc_subid + ";";
		count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		assertTrue("Subscriber id '" + pc_subid + "' should be removed from the subscriber table.", count == 0);
		
		stmt = "SELECT count(*) FROM APPLICATION.TEST_SITUATION_DATA_FILTER WHERE ID=" + pc_subid + " AND REQUEST_ID=1001;";
		count = session.executeSingleValueQuery(stmt, null, SqlTypeHandler.INTEGER);
		assertTrue("Subscriber id '" + pc_subid + "' and request id '1001' should be removed from the situation data filter table.", count == 0);
		
		processor.dispose();
	}
	
	private List<ByteBuffer> collectResponses(int expected) {
		ArrayList<ByteBuffer> responses = new ArrayList<ByteBuffer>();
		while (expected > 0) try {
			ByteBuffer response = queue.poll(100, TimeUnit.MILLISECONDS);
			if (response != null) responses.add(response);
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			expected--;
		}
		return responses;
	}
	
}