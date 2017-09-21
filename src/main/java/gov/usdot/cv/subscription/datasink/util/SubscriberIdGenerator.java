package gov.usdot.cv.subscription.datasink.util;

import gov.usdot.cv.common.model.Subscriber;
import gov.usdot.cv.common.subscription.response.ResponseCode;
import gov.usdot.cv.subscription.datasink.dao.DaoManager;
import gov.usdot.cv.subscription.datasink.exception.SubscriptionException;

import java.util.BitSet;
import java.util.Collection;

public class SubscriberIdGenerator {
	private static final int MIN = 10000000;
	private static final int MAX = 99999999;
	
	private BitSet used = new BitSet(MAX - MIN);
	private int currId = MIN;
	
	private static class SubscriberIdGeneratorHolder {
		public static final SubscriberIdGenerator INSTANCE = new SubscriberIdGenerator();
	}

	public static SubscriberIdGenerator getInstance() {
		return SubscriberIdGeneratorHolder.INSTANCE;
	}

	private SubscriberIdGenerator() {
		// Prevent instantiation from other classes
	}
	
	/** 
	 * Get the next available subscriber id.
	 */
	public synchronized int nextId() throws SubscriptionException {
		if (this.currId > MAX) {
			this.currId = MIN;	
		}
		
		if (this.currId == MIN) {
			loadSubscriberIds();
		}
			
		while (this.currId <= MAX) {
			try {
				int idx = this.currId - MIN;
				if (! this.used.get(idx)) {
					this.used.set(idx);
					return this.currId;
				}
			} finally {
				this.currId++;
			}
		}
		
		throw new SubscriptionException("Subscriber ids have been exhausted.", ResponseCode.ResourceLimitReached);
	}
	
	/**
	 * Release the given subscriber id.
	 */
	public synchronized void release(int id) {
		if (id < MIN && id > MAX) return;
		if (id < this.currId) this.currId = id;
		this.used.set(id - MIN, false);
	}
	
	private synchronized void loadSubscriberIds() {
		Collection<Subscriber> subscribers = DaoManager.getInstance().getSubscriberDao().findAll();
		if (subscribers != null && subscribers.size() > 0) {
			for (Subscriber subscriber : subscribers) {
				this.used.set(subscriber.getSubscriberId() - MIN);
			}
		}
	}
}