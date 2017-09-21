package gov.usdot.cv.subscription.datasink.receiver;

import gov.usdot.asn1.generated.j2735.J2735;
import gov.usdot.asn1.generated.j2735.semi.DataSubscriptionResponse;
import gov.usdot.asn1.generated.j2735.semi.GeoRegion;
import gov.usdot.asn1.generated.j2735.semi.ServiceResponse;
import gov.usdot.asn1.j2735.J2735Util;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

import com.oss.asn1.AbstractData;
import com.oss.asn1.Coder;

public class SubscriptionResponseReceiver implements Runnable {

	private Coder coder;
	private BlockingQueue<ByteBuffer> queue;
	private DatagramSocket socket;
	
	public SubscriptionResponseReceiver(int port, Coder coder) throws SocketException {
		this.coder = coder;
		this.socket = new DatagramSocket(port);
		this.socket.setSoTimeout(2 * 60 * 1000);
	}
	
	public SubscriptionResponseReceiver(
			BlockingQueue<ByteBuffer> queue, 
			int port) throws SocketException {
		this.queue = queue;
		this.socket = new DatagramSocket(port);
	}
	
	public void run() {
		System.out.println("Starting subscription receiver ...");
		 while (true) try {
			byte [] buffer = new byte[10000];
			DatagramPacket dp = new DatagramPacket(buffer, buffer.length);
			this.socket.receive(dp);
			if (this.queue != null) {
				this.queue.offer(ByteBuffer.wrap(buffer));
			} else {
				AbstractData response = J2735Util.decode(this.coder, buffer);
				if (response instanceof ServiceResponse) {
					System.out.println("Printing DPCServiceResponse ...");
					System.out.println("  Dialog ID		: " + ((ServiceResponse) response).getDialogID().getFirstNumber());
					System.out.println("  Sequence ID	: " + ((ServiceResponse) response).getSeqID().getFirstNumber());
					System.out.println("  Request ID	: " + ((ServiceResponse) response).getRequestID());
					System.out.println("  Hash			: " + ((ServiceResponse) response).getHash());
					GeoRegion region = ((ServiceResponse) response).getServiceRegion();
					if (region != null) {
						System.out.println("  NW Corner		: " + region.getNwCorner());
						System.out.println("  SE Corner		: " + region.getSeCorner());
					}
				} else if (response instanceof DataSubscriptionResponse) {
					System.out.println("Printing DPCSubscriptionResponse ...");
					System.out.println("  Dialog ID		: " + ((DataSubscriptionResponse) response).getDialogID().getFirstNumber());
					System.out.println("  Sequence ID	: " + ((DataSubscriptionResponse) response).getSeqID().getFirstNumber());
					System.out.println("  Request ID	: " + ((DataSubscriptionResponse) response).getRequestID());
					System.out.println("  Subscriber ID : " + ((DataSubscriptionResponse) response).getSubID());
					System.out.println("  Error Code	: " + ((DataSubscriptionResponse) response).getErr());
				} else {
					System.out.println("Received an unknown response: " + response);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public static void main(String [] args) throws Exception {
		if (args.length != 1) {
			System.out.println("usage: java SubscriptionResponseReceiver <port>");
			System.exit(1);
		}
		
		J2735.initialize();
		Thread t = new Thread(new SubscriptionResponseReceiver(Integer.parseInt(args[0]), J2735.getPERUnalignedCoder()));
		t.start();
		t.join();
	}
	
}