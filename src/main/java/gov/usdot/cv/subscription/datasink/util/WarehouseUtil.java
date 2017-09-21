package gov.usdot.cv.subscription.datasink.util;

import gov.usdot.cv.common.model.BoundingBox;
import gov.usdot.cv.common.subscription.response.ResponseCode;
import gov.usdot.cv.common.util.InstanceMetadataUtil;
import gov.usdot.cv.subscription.datasink.exception.SubscriptionException;

public class WarehouseUtil {
	
	private static BoundingBox supportedRegion = null;

	private WarehouseUtil() {
		// All method invocation goes through static methods
	}
	
	public static int getNodeNumber() throws SubscriptionException {
		int num = InstanceMetadataUtil.getNodeNumber();
		if (num == -1) {
			throw new SubscriptionException("Missing system properties 'RTWS_FQDN' and/or 'RTWS_DOMAIN'.", 
				ResponseCode.InternalServerError);
		}
		return num;
	}
	
	public static synchronized void setSupportedRegion(
			double nwLat, 
			double nwLon, 
			double seLat, 
			double seLon) {
			if (supportedRegion == null) {
				BoundingBox.Builder builder = new BoundingBox.Builder();
				builder.setNWLat(nwLat).setNWLon(nwLon).setSELat(seLat).setSELon(seLon);
				supportedRegion = builder.build();
			}
	}
	
	public static boolean withinSupportedRegion(
			double lat, 
			double lon) throws SubscriptionException {
		if (supportedRegion == null) {
			throw new SubscriptionException("Supported region not set.", ResponseCode.InternalServerError);
		}
		
		// The bounding box for the United States (excluding Hawaii) is
		//		NW Lat: 49.384358, Lon: -124.848974
		//		SE Lat: 24.396308, Lon: -66.885444
		// So we don't need to worry about regions that cross
		// negative to positive latitude and longitude ranges.
		
		if (Double.compare(lat, supportedRegion.getSELat()) < 0 ||
			Double.compare(lat, supportedRegion.getNWLat()) > 0 ||
			Double.compare(lon, supportedRegion.getNWLon()) < 0 ||
			Double.compare(lon, supportedRegion.getSELon()) > 0) {
			return  false;
		}
		
		return true;
	}
	
}