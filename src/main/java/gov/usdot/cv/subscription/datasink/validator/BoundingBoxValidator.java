package gov.usdot.cv.subscription.datasink.validator;

import gov.usdot.cv.common.model.BoundingBox;
import gov.usdot.cv.common.subscription.response.ResponseCode;
import gov.usdot.cv.subscription.datasink.exception.SubscriptionException;
import gov.usdot.cv.subscription.datasink.util.WarehouseUtil;

public class BoundingBoxValidator {
	
	private BoundingBoxValidator() {
		// All method invocation goes through static methods
	}
	
	public static BoundingBox validate(BoundingBox bb) throws SubscriptionException {
		if (bb == null) {
			throw new SubscriptionException("BoundingBox object is null.", ResponseCode.InternalServerError);
		}
		
		if (bb.getNWLat() == null) {
			throw new SubscriptionException("NW latitude is not set.", ResponseCode.NWLatMissing);
		}
		
		if (bb.getNWLon() == null) {
			throw new SubscriptionException("NW longitude is not set.", ResponseCode.NWLonMissing);
		}
		
		if (bb.getSELat() == null) {
			throw new SubscriptionException("SE latitude is not set.", ResponseCode.SELatMissing);
		}
		
		if (bb.getSELon() == null) {
			throw new SubscriptionException("SE longitude is not set.", ResponseCode.SELonMissing);
		}
		
		if (bb.getNWLat() > BoundingBox.MAX_LAT || bb.getNWLat() < BoundingBox.MIN_LAT) {
			throw new SubscriptionException("Invalid NW latitude value.", ResponseCode.InvalidBoundingBox);
		}
		
		if (bb.getNWLon()> BoundingBox.MAX_LON || bb.getNWLon() < BoundingBox.MIN_LON) {
			throw new SubscriptionException("Invalid NW longitude value.", ResponseCode.InvalidBoundingBox);
		}
		
		if (bb.getSELat() > BoundingBox.MAX_LAT || bb.getSELat() < BoundingBox.MIN_LAT) {
			throw new SubscriptionException("Invalid SE latitude value.", ResponseCode.InvalidBoundingBox);
		}
		
		if (bb.getSELon() > BoundingBox.MAX_LON || bb.getSELon() < BoundingBox.MIN_LON) {
			throw new SubscriptionException("Invalid SE longitude value.", ResponseCode.InvalidBoundingBox);
		}
		
		if (bb.getNWLat() < bb.getSELat() || bb.getNWLon() > bb.getSELon()) {
			throw new SubscriptionException("NW and SE positions doesn't form a bounding box.", ResponseCode.InvalidBoundingBox);
		}
		
		if (! WarehouseUtil.withinSupportedRegion(bb.getNWLat(), bb.getNWLon())) {
			throw new SubscriptionException("NW position is not within the supported region.", ResponseCode.InvalidBoundingBox);
		}
		
		if (! WarehouseUtil.withinSupportedRegion(bb.getSELat(), bb.getSELon())) {
			throw new SubscriptionException("SE position is not within the supported region.", ResponseCode.InvalidBoundingBox);
		}
		
		return bb;
	}
	
}