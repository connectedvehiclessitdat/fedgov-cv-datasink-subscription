package gov.usdot.cv.subscription.datasink.util;

import java.io.ByteArrayOutputStream;

import com.oss.asn1.AbstractData;
import com.oss.asn1.Coder;
import com.oss.asn1.ControlTableNotFoundException;
import com.oss.asn1.EncodeFailedException;
import com.oss.asn1.EncodeNotSupportedException;
import com.oss.asn1.InitializationException;

import gov.usdot.asn1.generated.j2735.J2735;

public class J2735Coder {
	private static final J2735Coder INSTANCE;
	
	static {
		try {
			INSTANCE = new J2735Coder();
		} catch (Exception ex) {
			throw new RuntimeException("Failed to instaniate J2735 coder.", ex);
		}
	}
	
	private Coder coder;
	
	public static J2735Coder getInstance()  {
		return INSTANCE;
	}
	
	private J2735Coder() throws ControlTableNotFoundException, InitializationException {
		J2735.initialize();
		coder = J2735.getPERUnalignedCoder();
	}
	
	public byte [] encode(AbstractData message) 
			throws EncodeFailedException, EncodeNotSupportedException {
		ByteArrayOutputStream sink = new ByteArrayOutputStream();
		this.coder.encode(message, sink);
		return sink.toByteArray();
	}
}