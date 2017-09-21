package gov.usdot.cv.subscription.datasink;

import gov.usdot.cv.common.inet.InetPoint;
import gov.usdot.cv.common.model.BoundingBox;
import gov.usdot.cv.common.model.Filter;
import gov.usdot.cv.common.model.Subscriber;
import gov.usdot.cv.common.subscription.response.ResponseCode;
import gov.usdot.cv.common.util.Syslogger;
import gov.usdot.cv.subscription.datasink.dao.DaoManager;
import gov.usdot.cv.subscription.datasink.exception.SubscriptionException;
import gov.usdot.cv.subscription.datasink.expiration.ExpirationProcessor;
import gov.usdot.cv.subscription.datasink.model.Cancellation;
import gov.usdot.cv.subscription.datasink.model.DataModel;
import gov.usdot.cv.subscription.datasink.response.Response;
import gov.usdot.cv.subscription.datasink.response.ResponseSender;
import gov.usdot.cv.subscription.datasink.util.DatabaseUtil;
import gov.usdot.cv.subscription.datasink.util.SubscriberIdGenerator;
import gov.usdot.cv.subscription.datasink.util.WarehouseUtil;
import gov.usdot.cv.subscription.datasink.validator.BoundingBoxValidator;
import gov.usdot.cv.subscription.datasink.validator.FilterValidator;
import gov.usdot.cv.subscription.datasink.validator.SubscriberValidator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.concurrent.LinkedBlockingQueue;

import javax.sql.DataSource;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import net.sf.json.JSONObject;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import com.deleidos.rtws.commons.exception.InitializationException;
import com.deleidos.rtws.core.framework.Description;
import com.deleidos.rtws.core.framework.SystemConfigured;
import com.deleidos.rtws.core.framework.UserConfigured;
import com.deleidos.rtws.core.framework.processor.AbstractDataSink;

@Description("Process vehicle situation data subscription request.")
public class SubscriptionProcessor extends AbstractDataSink {
	
	private final static String SYS_LOG_ID = "UDP SubscriptionProcessor";
	
	private final Logger logger = Logger.getLogger(getClass());

	private DataSource dataSource;
	private String subscriberTableName;
	private String filterTableName;
	
	private static final Object LOCK = new Object();
	private static LinkedBlockingQueue<Response> queue;
	private static ResponseSender sender;
	private static Thread sender_t;
	
	private static ExpirationProcessor processor;
	private static Thread processor_t;
	
	private double nwLat;
	private double nwLon;
	private double seLat;
	private double seLon;
	private String forwarderHost;
	private int forwarderPort;
	
	public SubscriptionProcessor() {
		super();
	}
	
	@Override
	@SystemConfigured(value = "Situtation Data Subscription Processor")
	public void setName(String name) {
		super.setName(name);
	}
	
	@Override
	@SystemConfigured(value = "cvsubscription")
	public void setShortname(String shortname) {
		super.setShortname(shortname);
	}
	
	@SystemConfigured(value="java:com.deleidos.rtws.commons.dao.source.H2ConnectionPool <URL>@h2.app.connection.url@</URL><user>@h2.app.connection.user@</user><password>@h2.app.connection.password@</password>")
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}
	
	public DataSource getDataSource() {
		return this.dataSource;
	}
	
	@UserConfigured(value = "SUBSCRIBER", description = "The name of the subscriber table.")
	public void setDatabaseSubscriberTableName(String subscriberTableName) {
		this.subscriberTableName = subscriberTableName;
	}
	
	public String getDatabaseSubscriberTableName() {
		return this.subscriberTableName;
	}
	
	@UserConfigured(value = "SITUATION_DATA_FILTER", description = "The name of the situation data filter table.")
	public void setDatabaseFilterTableName(String filterTableName) {
		this.filterTableName = filterTableName;
	}
	
	public String getDatabaseFilterTableName() {
		return this.filterTableName;
	}
	
	@UserConfigured(value = "43.0", description = "The northwest latitude of the service region.", 
			flexValidator = { "NumberValidator minValue=-90.0 maxValue=90.0" })
	public void setNorthwestLatitude(double nwLat) {
		this.nwLat = nwLat;
	}
	
	public double getNorthwestLatitude() {
		return this.nwLat;
	}
	
	@UserConfigured(value = "-85.0", description = "The northwest longitude of the service region.", 
			flexValidator = { "NumberValidator minValue=-180.0 maxValue=180.0" })
	public void setNorthwestLongitude(double nwLon) {
		this.nwLon = nwLon;
	}
	
	public double getNorthwestLongitude() {
		return this.nwLon;
	}
	
	@UserConfigured(value = "41.0", description = "The southeast latitude of the service region.", 
			flexValidator = { "NumberValidator minValue=-90.0 maxValue=90.0" })
	public void setSoutheastLatitude(double seLat) {
		this.seLat = seLat;
	}
	
	public double getSoutheastLatitude() {
		return this.seLat;
	}
	
	@UserConfigured(value = "-82.0", description = "The southeast longitude of the service region.", 
			flexValidator = { "NumberValidator minValue=-180.0 maxValue=180.0" })
	public void setSoutheastLongitude(double seLon) {
		this.seLon = seLon;
	}
	
	public double getSoutheastLongitude() {
		return this.seLon;
	}
	
	@UserConfigured(value = "127.0.0.1", description = "The forwarder host.", flexValidator = { "StringValidator minLength=2 maxLength=1024" })
	public void setForwarderHost(String forwarderHost) {
		this.forwarderHost = forwarderHost;
	}

	@NotNull
	public String getForwarderHost() {
		return this.forwarderHost;
	}

	@UserConfigured(value = "46761", description = "The forwarder port number.", flexValidator = "NumberValidator minValue=0 maxValue=65535")
	public void setForwarderPort(int forwarderPort) {
		this.forwarderPort = forwarderPort;
	}

	@Min(0)
	@Max(65535)
	public int getForwarderPort() {
		return this.forwarderPort;
	}
	
	public void initialize() throws InitializationException {
		WarehouseUtil.setSupportedRegion(this.nwLat, this.nwLon, this.seLat, this.seLon);

		logger.info("Building subscription tables ...");
		DatabaseUtil.buildSubscriptionTables(this.dataSource, this.subscriberTableName, this.filterTableName);

		logger.info("Initializing data access objects ...");
		DaoManager.getInstance().setAndInitialize(this.dataSource, this.subscriberTableName, this.filterTableName);
		
		synchronized (LOCK) {
			logger.info("Initializing response sender ...");
			queue = new LinkedBlockingQueue<Response>();
			InetPoint forwarderPoint = null;
			if (forwarderHost != null && forwarderPort != 0) {
				try {
					forwarderPoint = new InetPoint(InetAddress.getByName(forwarderHost).getAddress(),forwarderPort);
				} catch (UnknownHostException e) {
					logger.error("Error creating forwarder InetPoint ", e);
				}
			}
			sender = new ResponseSender(queue,forwarderPoint);
			sender_t = new Thread(sender);
			sender_t.start();
		
			try {
				if (processor_t == null && WarehouseUtil.getNodeNumber() == 1) {
					logger.info("Initializing expiration processor ...");
					processor = new ExpirationProcessor();
					processor_t = new Thread(processor);
					processor_t.start();
				}
			} catch (Exception ex) {
				logger.error("Failed instantiate expiration processor thread.",ex);
			}
		}
		
		logger.info("Subscription processor datasink initialized.");
	}
	
	public void dispose() {
		synchronized (LOCK) {
			if (sender != null && sender_t != null) {
				sender.terminate();
				try { sender_t.join(5000); } catch (InterruptedException e) {}
				sender = null;
				sender_t = null;
			}
			if (processor != null && processor_t != null) {
				processor.terminate();
				try { processor_t.join(5000); } catch (InterruptedException e) {}
				processor = null;
				processor_t = null;
			}
		}
		logger.info("Subscription processor datasink disposed.");
	}
	
	public void flush() {
		logger.debug(String.format("The method flush() is not used by '%s'.", this.getClass().getName()));
	}

	@Override
	protected void processInternal(JSONObject record, FlushCounter counter) {
		DataModel model = new DataModel(record);
		try {
			if (model.isSubscriptionRequest()) {
				logger.debug(String.format("Adding subscription %s", model.toString()));
				processAddRequest(model);
				Syslogger.getInstance().log(SYS_LOG_ID, 
						String.format("Added subscription %s", model.toString()));
			} else if (model.isSubscriptionCancel()) {
				logger.debug(String.format("Cancelling subscription %s", model.toString()));
				processCancelRequest(model);
				Syslogger.getInstance().log(SYS_LOG_ID, 
						String.format("Cancelled subscription %s", model.toString()));
			} else {
				logger.debug(String.format("Invalid subscription request %s", model.toString()));
				Syslogger.getInstance().log(SYS_LOG_ID, 
						String.format("Invalid subscription request %s", model.toString()));
				processInvalidRequest(model);
			}
		} catch (Exception ex) {
			if (model.getCertificate() != null) record.remove(DataModel.CERTIFICATE_KEY);
			logger.error(String.format("Failed to process subscription request: %s", record.toString()), ex);
			Syslogger.getInstance().log(SYS_LOG_ID, 
					String.format("Failed to process subscription request %s", record.toString()));
		} finally {
			counter.noop();
		}
	}
	
	private void processAddRequest(DataModel model) throws Exception {
		Subscriber subscriber = null;
		
		try {
			subscriber = buildSubscriber(model);
			DaoManager.getInstance().getSubscriberDao().upsert(subscriber.getSubscriberId(), subscriber);
			DaoManager.getInstance().getSituationDataFilterDao().insert(subscriber.getSubscriberId(), subscriber.getFilter());
			buildAndEnqueueResponse(
					subscriber.getSubscriberId(),
					subscriber.getFilter().getRequestId(),
					subscriber.getDestHost(), 
					subscriber.getDestPort(),
					null,
					subscriber.getCertificate(), 
					model.fromForwarder());
		} catch (Exception ex) {
			int subscriberId = (subscriber != null) ? subscriber.getSubscriberId() : 0;
			int requestId = (subscriber != null && subscriber.getFilter() != null) ? subscriber.getFilter().getRequestId() : 0;
			String targetHost = (subscriber != null) ? subscriber.getDestHost() : model.getDestHost();
			Integer targetPort = (subscriber != null) ? subscriber.getDestPort() : model.getDestPort();
			byte[] certficate = (subscriber != null) ? subscriber.getCertificate() : null;
			boolean fromForwarder = (model.fromForwarder() != null) ? model.fromForwarder() : false;
			processExceptionAndEnqueueResponse(subscriberId, requestId, targetHost, targetPort, ex, certficate, fromForwarder);
			throw ex;
		}
	}
	
	private void processCancelRequest(DataModel model) throws Exception {
		Cancellation cancellation = null;
		Subscriber subscriber = null;
		Filter filter = null;
		
		try {
			cancellation = buildCancellation(model);
			subscriber = DaoManager.getInstance().getSubscriberDao().findById(cancellation.getSubscriberId());
			filter = DaoManager.getInstance().getSituationDataFilterDao().findById(cancellation.getSubscriberId());
			
			if (filter != null) {
				if (filter.getRequestId() != cancellation.getRequestId()) {
					throw new SubscriptionException("Invalid request id attribute in record.", ResponseCode.InvalidRequestId);
				} else {
					DaoManager.getInstance().getSituationDataFilterDao().delete(cancellation.getSubscriberId(), cancellation.getRequestId());
				}
			}
			
			if (subscriber != null) {
				DaoManager.getInstance().getSubscriberDao().delete(cancellation.getSubscriberId());
				SubscriberIdGenerator.getInstance().release(subscriber.getSubscriberId());
			}
			
			buildAndEnqueueResponse(
					subscriber.getSubscriberId(),
					filter.getRequestId(),
					subscriber.getDestHost(), 
					subscriber.getDestPort(),
					null,
					subscriber.getCertificate(), 
					model.fromForwarder());
		} catch (Exception ex) {
			int subscriberId = (subscriber != null) ? subscriber.getSubscriberId() : 0;
			int requestId = (filter != null) ? filter.getRequestId() : 0;
			String targetHost = (subscriber != null) ? subscriber.getDestHost() : null;
			Integer targetPort = (subscriber != null) ? subscriber.getDestPort() : null;
			byte[] certficate = (subscriber != null) ? subscriber.getCertificate() : null;
			boolean fromForwarder = (model.fromForwarder() != null) ? model.fromForwarder() : false;
			processExceptionAndEnqueueResponse(subscriberId, requestId, targetHost, targetPort, ex, certficate, fromForwarder);
			throw ex;
		}
	}
	
	private void processInvalidRequest(DataModel model) throws Exception {
		Integer subscriberId = model.getSubscriberId() == null ? 0 : model.getSubscriberId();
		Integer requestId = model.getRequestId() == null ? 0 : model.getRequestId();
		String destHost = model.getDestHost();
		Integer destPort = model.getDestPort();
		
		ResponseCode rc = null;
		if (destHost == null || destPort == null) {
			logger.warn("Failed to build subscription response because destination host and port is not available.");
		} else if (model.getSemiDialogID() == null) {
			rc = ResponseCode.DialogIDMissing;
		} else if (! model.isSemiDialogIdValid()) {
			rc = ResponseCode.InvalidDialogID;
		} else if (model.getSemiSequenceID() == null) {
			rc = ResponseCode.SequenceIDMissing;
		} else if (! model.isSemiSequenceIdValid()) {
			rc = ResponseCode.InvalidSequenceID;
		} else if (subscriberId == 0) {
			rc = ResponseCode.SubscriberIdMissing;
		} else if (requestId == 0) {
			rc = ResponseCode.RequestIdMissing;
		} 
		
		String cert = model.getCertificate();
		byte[] certificate = cert != null ? Base64.decodeBase64(cert): null;
		boolean fromForwarder = (model.fromForwarder() != null) ? model.fromForwarder() : false;
		buildAndEnqueueResponse(subscriberId, requestId, destHost, destPort, rc, certificate, fromForwarder);
		
		throw new SubscriptionException("Invalid subscription request.");
	}
	
	private void processExceptionAndEnqueueResponse(
			int subscriberId, 
			int requestId,
			String targetHost, 
			Integer targetPort, 
			Exception ex,
			byte[] certificate,
			boolean fromForwarder) {
		if (targetHost == null || targetPort == null) {
			logger.warn("Failed to build subscription response because target host and port is not available.");
		} else {
			ResponseCode code = (ex instanceof SubscriptionException) ? 
				((SubscriptionException) ex).getResponseCode() : ResponseCode.InternalServerError;
			buildAndEnqueueResponse(subscriberId, requestId, targetHost, targetPort, code, certificate, fromForwarder);
		}
	}
	
	private void buildAndEnqueueResponse(
			int subscriberId,
			int requestId,
			String targetHost, 
			int targetPort,
			ResponseCode code,
			byte[] certificate,
			boolean fromForwarder) {
		Response.Builder builder = new Response.Builder();
		builder.setSubscriberId(subscriberId);
		builder.setTargetHost(targetHost);
		builder.setTargetPort(targetPort);
		builder.setRequestId(requestId);
		builder.setResponseCode(code);
		builder.setCertificate(certificate);
		builder.setFromForwarder(fromForwarder);
		queue.offer(builder.build());
	}
	
	private Subscriber buildSubscriber(DataModel model) throws Exception {
		Integer subscriberId = null;
		
		try {
			String certificate = model.getCertificate();
			String destHost = model.getDestHost();
			Integer destPort = model.getDestPort();
			
			if (destHost == null) {
				throw new SubscriptionException("Missing destination host attribute in record.", ResponseCode.TargetHostMissing);
			}
			
			if (destPort == null) {
				throw new SubscriptionException("Missing destination port attribute in record.", ResponseCode.TargetPortMissing);
			}
			
			subscriberId = SubscriberIdGenerator.getInstance().nextId();
			Filter filter = buildFilter(subscriberId, model);
			
			Subscriber.Builder builder = new Subscriber.Builder();
			builder
				.setSubscriberId(subscriberId)
				.setCertificate(certificate != null ? Base64.decodeBase64(certificate): null)
				.setDestHost(destHost)
				.setDestPort(destPort)
				.setFilter(filter);
			return SubscriberValidator.validate(builder.build());
		} catch (Exception ex) {
			if (subscriberId != null) SubscriberIdGenerator.getInstance().release(subscriberId);
			throw ex;
		}
	}
	
	private Filter buildFilter(int subscriberId, DataModel model) throws SubscriptionException, ParseException {
		BoundingBox boundingBox = buildBoundingBox(model);
		
		String endTime = model.getEndTime();
		String type = model.getType();
		Integer typeValue = model.getTypeValue();
		Integer requestId = model.getRequestId();
		
		if (endTime == null) {
			throw new SubscriptionException("Missing end time attribute in record.", ResponseCode.EndTimeMissing);
		}

		if (type == null) {
			throw new SubscriptionException("Missing type attribute in record.", ResponseCode.TypeMissing);
		}
		
		if (typeValue == null) {
			throw new SubscriptionException("Missing type attribute in record.", ResponseCode.TypeValueMissing);
		}
		
		if (requestId == null) {
			throw new SubscriptionException("Missing request id attribute in record.", ResponseCode.RequestIdMissing);
		}
		
		Filter.Builder builder = new Filter.Builder();
		builder
			.setSubscriberId(subscriberId)
			.setEndTime(endTime)
			.setType(type)
			.setTypeValue(typeValue)
			.setRequestId(requestId)
			.setBoundingBox(boundingBox);
		return FilterValidator.validate(builder.build());
	}
	
	private BoundingBox buildBoundingBox(DataModel model) throws SubscriptionException {
		if (! model.hasNWPosObj() && ! model.hasSEPosObj()) {
			return null;
		}
		
		if (model.isBoundingBoxEmpty()) {
			return null;
		}
		
		if (model.hasNWPosObj() && ! model.hasSEPosObj()) {
			throw new SubscriptionException("Missing southeast position attribute in record.", ResponseCode.SEPosMissing);
		}
		
		if (! model.hasNWPosObj() && model.hasSEPosObj()) {
			throw new SubscriptionException("Missing northwest position attribute in record.", ResponseCode.NWPosMissing);
		}
		
		Double nwLat = model.getNWLat();
		Double nwLon = model.getNWLon();
		Double seLat = model.getSELat();
		Double seLon = model.getSELon();
			
		if (nwLat == null) {
			throw new SubscriptionException("Missing northwest latitude attribute in record.", ResponseCode.NWLatMissing);
		}
			
		if (nwLon == null) {
			throw new SubscriptionException("Missing northwest longitude attribute in record.", ResponseCode.NWLonMissing);
		}
			
		if (seLat == null) {
			throw new SubscriptionException("Missing southeast latitude attribute in record.", ResponseCode.SELatMissing);
		}
			
		if (seLon == null) {
			throw new SubscriptionException("Missing southeast longitude attribute in record.", ResponseCode.SELonMissing);
		}
			
		BoundingBox.Builder builder = new BoundingBox.Builder();
		builder.setNWLat(nwLat).setNWLon(nwLon).setSELat(seLat).setSELon(seLon);
		return BoundingBoxValidator.validate(builder.build());
	}
	
	private Cancellation buildCancellation(DataModel model) throws SubscriptionException {
		Integer subscriberId = model.getSubscriberId();
		Integer requestId = model.getRequestId();
		
		if (subscriberId == null) {
			throw new SubscriptionException("Missing subscriber id attribute in record.", ResponseCode.SubscriberIdMissing);
		}
		
		if (requestId == null) {
			throw new SubscriptionException("Missing request id attribute in record.", ResponseCode.RequestIdMissing);
		}
		
		Cancellation.Builder builder = new Cancellation.Builder();
		builder.setSubscriberId(subscriberId).setRequestId(requestId);
		return builder.build();
	}
	
}