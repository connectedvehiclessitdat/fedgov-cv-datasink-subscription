package gov.usdot.cv.subscription.datasink.exception;

import gov.usdot.cv.common.subscription.response.ResponseCode;

public class SubscriptionException extends Exception {

	private static final long serialVersionUID = 3795841306615351089L;

	private ResponseCode code;
	
	public SubscriptionException(String message) {
		super(message);
	}
	
	public SubscriptionException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public SubscriptionException(String message, ResponseCode code) {
		super(message);
		this.code = code;
	}
	
	public SubscriptionException(String message, Throwable cause, ResponseCode code) {
		super(message, cause);
		this.code = code;
	}
	
	public ResponseCode getResponseCode() {
		return this.code;
	}
}