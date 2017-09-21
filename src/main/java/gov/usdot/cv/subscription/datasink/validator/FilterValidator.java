package gov.usdot.cv.subscription.datasink.validator;

import gov.usdot.cv.common.model.Filter;
import gov.usdot.cv.common.subscription.response.ResponseCode;
import gov.usdot.cv.subscription.datasink.exception.SubscriptionException;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public class FilterValidator {
	
	private FilterValidator() {
		// All method invocation goes through static methods
	}
	
	public static Filter validate(Filter filter) throws SubscriptionException {
		if (filter == null) {
			throw new SubscriptionException("Filter object is null.", ResponseCode.InternalServerError);
		}
		
		if (filter.getEndTime() == null) {
			throw new SubscriptionException("End time is not set.", ResponseCode.EndTimeMissing);
		}
		
		if (filter.getType() == null) {
			throw new SubscriptionException("Type is not set.", ResponseCode.TypeMissing);
		}
		
		if (filter.getTypeValue() == null) {
			throw new SubscriptionException("Type value is not set.", ResponseCode.TypeMissing);
		}
		
		if (filter.getRequestId() == null) {
			throw new SubscriptionException("Request id is not set.", ResponseCode.RequestIdMissing);
		}

		// Verify the end time is after the current time
		Calendar current = Calendar.getInstance(TimeZone.getTimeZone(Filter.UTC_TIMEZONE));
		if (filter.getEndTime().before(current)) {
			DateFormat formatter = new SimpleDateFormat(Filter.DATE_PATTERN);
			throw new SubscriptionException("End time can't be before current time '" + formatter.format(current.getTime()) + "'.", ResponseCode.InvalidEndTime);
		}
		
		return filter;
	}
	
}