package gov.usdot.cv.subscription.datasink.response;

import gov.usdot.cv.common.inet.InetPacketException;
import gov.usdot.cv.common.inet.InetPacketSender;
import gov.usdot.cv.common.inet.InetPoint;
import gov.usdot.cv.security.SecurityHelper;
import gov.usdot.cv.security.crypto.CryptoProvider;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.oss.asn1.EncodeFailedException;
import com.oss.asn1.EncodeNotSupportedException;

public class ResponseSender implements Runnable {
	private final Logger logger = Logger.getLogger(getClass());
	private final BlockingQueue<Response> queue;
	private boolean terminated = false;
	private final CryptoProvider cryptoProvider = new CryptoProvider();
	private static final int Psid = 0x2fe1;
	private InetPacketSender packetSender;
	
	public ResponseSender(BlockingQueue<Response> queue, InetPoint forwarderPoint) {
		this.queue = queue;
		packetSender = new InetPacketSender(forwarderPoint);
		SecurityHelper.initSecurity();
	}
	
	public void terminate() {
		this.terminated = true;
		SecurityHelper.disposeSecurity();
	}
	
	public void run() {
		logger.info("Response sender [" + Thread.currentThread().getId() + "] is starting ...");
		logger.info("Response sender [" + Thread.currentThread().getId() + "] is ready for work ...");
		loopAndPoll();
	}
	
	private void loopAndPoll() {
		while (! this.terminated || this.queue.size() > 0) try {
			send(this.queue.poll(1, TimeUnit.SECONDS));
		} catch (InterruptedException ie) {
			logger.error("Response sender was interrupted.", ie);
		} catch (Exception ex) {
			logger.error("Failed to send response.", ex);
		}
	}
	
	private void send(Response response) throws EncodeFailedException, EncodeNotSupportedException, IOException, InetPacketException {
		if (response == null) return;
		
		byte [] payload = response.encode();
		if (response.getCertificate() != null) {
			try {
				byte[] certID8 = SecurityHelper.registerCert(response.getCertificate(), cryptoProvider);
				payload = SecurityHelper.encrypt(payload, certID8, cryptoProvider, Psid);
			} catch (Exception ex) {
				logger.error("Couldn't encrypt outgoing message. Reason: " + ex.getMessage(), ex);
			}
		}
		
		InetPoint destPoint = new InetPoint(response.getTargetHost(), response.getTargetPort(), false);
		packetSender.forward(destPoint, payload, response.isFromForwarder());
	}
}