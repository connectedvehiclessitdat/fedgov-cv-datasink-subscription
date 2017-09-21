package gov.usdot.cv.subscription.datasink.model;

public class Cancellation {
	private Integer subscriberId;
	private Integer requestId;
	
	private Cancellation(
			int subscriberId, 
			int requestId) {
		this.subscriberId = subscriberId;
		this.requestId = requestId;
	}
	
	public int getSubscriberId() 	{ return this.subscriberId.intValue(); }
	public int getRequestId() 		{ return this.requestId.intValue(); }
	
	public static class Builder {
		private Integer subscriberId;
		private Integer requestId;
		
		public Builder setSubscriberId(int subscriberId) { 
			this.subscriberId = subscriberId;
			return this; 
		}
		
		public Builder setRequestId(int requestId) { 
			this.requestId = requestId;
			return this; 
		}
		
		public Cancellation build() {
			return new Cancellation(
					this.subscriberId,
					this.requestId);
		}
	}
}