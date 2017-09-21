package gov.usdot.cv.subscription.datasink.response;

import gov.usdot.asn1.generated.j2735.dsrc.TemporaryID;
import gov.usdot.asn1.generated.j2735.semi.DataSubscriptionResponse;
import gov.usdot.asn1.generated.j2735.semi.GroupID;
import gov.usdot.asn1.generated.j2735.semi.SemiDialogID;
import gov.usdot.asn1.generated.j2735.semi.SemiSequenceID;
import gov.usdot.cv.common.asn1.GroupIDHelper;
import gov.usdot.cv.common.subscription.response.ResponseCode;
import gov.usdot.cv.subscription.datasink.util.J2735Coder;

import java.nio.ByteBuffer;

import com.oss.asn1.EncodeFailedException;
import com.oss.asn1.EncodeNotSupportedException;
import com.oss.asn1.INTEGER;

public class Response {
	private String targetHost;
	private int targetPort;
	private int subscriberId;
	private int groupId;
	private int requestId;
	private ResponseCode code;
	private byte[] certificate;
	private boolean fromForwarder;
	
	public Response(
			String targetHost,
			int targetPort,
			int subscriberId,
			int groupId,
			int requestId,
			ResponseCode code,
			byte[] certificate,
			boolean fromForwarder) {
		this.targetHost = targetHost;
		this.targetPort = targetPort;
		this.subscriberId = subscriberId;
		this.groupId = groupId;
		this.requestId = requestId;
		this.code = code;
		this.certificate = certificate;
		this.fromForwarder = fromForwarder;
	}
	
	public String getTargetHost() {
		return this.targetHost;
	}
	
	public int getTargetPort() {
		return this.targetPort;
	}
	
	public byte[] getCertificate() {
		return certificate;
	}
	
	public boolean isFromForwarder() {
		return fromForwarder;
	}

	public byte [] encode() throws EncodeFailedException, EncodeNotSupportedException {
		GroupID groupId = GroupIDHelper.toGroupID(this.groupId);
		TemporaryID requestId = new TemporaryID(ByteBuffer.allocate(4).putInt(this.requestId).array());
		TemporaryID subscriberId = new TemporaryID(ByteBuffer.allocate(4).putInt(this.subscriberId).array());
		
		if (this.code == null) {
			return J2735Coder.getInstance().encode(new DataSubscriptionResponse(
					SemiDialogID.dataSubscription,
					SemiSequenceID.subscriptinoResp,
					groupId,
					requestId,
					subscriberId));
		} else {
			return J2735Coder.getInstance().encode(new DataSubscriptionResponse(
					SemiDialogID.dataSubscription,
					SemiSequenceID.subscriptinoResp,
					groupId,
					requestId,
					subscriberId,
					new INTEGER(this.code.getCode())));
		}	
	}
	
	public static class Builder {
		private String targetHost;
		private int targetPort;
		private int subscriberId;
		private int groupId;
		private int requestId;
		private ResponseCode code;
		private byte[] certificate;
		private boolean fromForwarder;
		
		public Builder setTargetHost(String targetHost) {
			this.targetHost = targetHost;
			return this;
		}
		
		public Builder setTargetPort(int targetPort) {
			this.targetPort = targetPort;
			return this;
		}
		
		public Builder setSubscriberId(int subscriberId) {
			this.subscriberId = subscriberId;
			return this;
		}
		
		public Builder setGroupId(int groupId) {
			this.groupId = groupId;
			return this;
		}
		
		public Builder setRequestId(int requestId) {
			this.requestId = requestId;
			return this;
		}
		
		public Builder setResponseCode(ResponseCode code) {
			this.code = code;
			return this;
		}
		
		public Builder setCertificate(byte[] certificate) {
			this.certificate = certificate;
			return this;
		}
		
		public Builder setFromForwarder(boolean fromForwarder) {
			this.fromForwarder = fromForwarder;
			return this;
		}

		public Response build() {
			return new Response(
					this.targetHost,
					this.targetPort,
					this.subscriberId,
					this.groupId,
					this.requestId,
					this.code,
					this.certificate,
					this.fromForwarder);
		}
	}
}