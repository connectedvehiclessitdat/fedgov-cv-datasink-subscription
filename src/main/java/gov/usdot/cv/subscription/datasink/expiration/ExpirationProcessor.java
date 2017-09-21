package gov.usdot.cv.subscription.datasink.expiration;

import gov.usdot.cv.common.model.Filter;
import gov.usdot.cv.common.model.Subscriber;
import gov.usdot.cv.common.util.PropertyLocator;
import gov.usdot.cv.subscription.datasink.dao.DaoManager;
import gov.usdot.cv.subscription.datasink.util.SubscriberIdGenerator;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.log4j.Logger;

public class ExpirationProcessor implements Runnable {	
	private final Logger logger 		= Logger.getLogger(getClass());
	private final DateFormat formatter 	= new SimpleDateFormat(Filter.DATE_PATTERN);
	
	private int interval;
	private boolean terminated = false;
	
	public ExpirationProcessor() {
		this.interval = PropertyLocator.getInt("subscription.expiration.processor.interval", 60000);
	}
	
	public void terminate() {
		this.terminated = true;
	}
	
	public void run() {
		logger.info("Subscription expiration processor [" + Thread.currentThread().getId() + "] is starting ...");
		while (! this.terminated) try {
			Calendar current = Calendar.getInstance(TimeZone.getTimeZone(Filter.UTC_TIMEZONE));
			logger.debug(String.format("Analyzing subscription for expiration, current time is '%s' ...", this.formatter.format(current.getTime())));

			Collection<Subscriber> subscribers = DaoManager.getInstance().getSubscriberDao().findAll();
			Map<Integer, Filter> index = buildFilterIndex();
			for (Subscriber subscriber : subscribers) {
				int subscriberId = subscriber.getSubscriberId();
				Filter filter = index.get(subscriberId);
				if (filter == null) {
					logger.info(String.format("A filter is missing for subscriber '%s'. Expiring subscription now.", subscriberId));
					DaoManager.getInstance().getSubscriberDao().delete(subscriber.getSubscriberId());
					SubscriberIdGenerator.getInstance().release(subscriber.getSubscriberId());
				} else {
					int requestId = filter.getRequestId();
					Calendar endTime = filter.getEndTime();
					if (endTime != null && endTime.before(current)) {
						logger.info(String.format("Subscriber '%s' end time '%s' has been reached. Expiring subscription now.", 
							subscriberId, this.formatter.format(endTime.getTime())));
						DaoManager.getInstance().getSituationDataFilterDao().delete(subscriberId, requestId);
						DaoManager.getInstance().getSubscriberDao().delete(subscriberId);
						SubscriberIdGenerator.getInstance().release(subscriberId);
					}
					
					// TODO: Need to check if the certificate has expired and if so expire the subscription.
				}
			}
			try { Thread.sleep(this.interval); } catch (InterruptedException ignore) {}
		} catch (Exception ex) {
			logger.error("Failed to process subscription filters for expiration.", ex);
		}
	}
	
	private Map<Integer, Filter> buildFilterIndex() {
		Collection<Filter> filters = DaoManager.getInstance().getSituationDataFilterDao().findAll();
		Map<Integer, Filter> index = new HashMap<Integer, Filter>();
		for (Filter filter : filters) {
			index.put(filter.getSubscriberId(), filter);
		}
		return index;
	}
	
}