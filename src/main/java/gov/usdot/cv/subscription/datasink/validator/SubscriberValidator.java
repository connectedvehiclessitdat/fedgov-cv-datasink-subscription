package gov.usdot.cv.subscription.datasink.validator;

import gov.usdot.cv.common.model.Subscriber;
import gov.usdot.cv.common.subscription.response.ResponseCode;
import gov.usdot.cv.subscription.datasink.exception.SubscriptionException;

public class SubscriberValidator {
	
	private SubscriberValidator() {
		// All method invocation goes through static methods
	}
	
	public static Subscriber validate(Subscriber subscriber) throws SubscriptionException {
		if (subscriber == null) {
			throw new SubscriptionException("Subscriber object is null.", ResponseCode.InternalServerError);
		}
		
		if (subscriber.getSubscriberId() == null) {
			throw new SubscriptionException("Subscriber id is not set.", ResponseCode.SubscriberIdMissing);
		}
		
		if (subscriber.getDestHost() == null) {
			throw new SubscriptionException("Destination host is not set.", ResponseCode.TargetHostMissing);
		}
		
		if (subscriber.getDestPort() == null) {
			throw new SubscriptionException("Destination port is not set.", ResponseCode.TargetPortMissing);
		}
		
		if (subscriber.getFilter() == null) {
			throw new SubscriptionException("Filter is not set.", ResponseCode.InternalServerError);
		}
		
		return subscriber;
	}

}