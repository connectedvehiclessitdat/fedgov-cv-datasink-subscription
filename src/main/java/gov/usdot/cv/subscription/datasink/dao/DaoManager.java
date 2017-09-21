package gov.usdot.cv.subscription.datasink.dao;

import gov.usdot.cv.common.subscription.dao.SituationDataFilterDao;
import gov.usdot.cv.common.subscription.dao.SubscriberDao;

import javax.sql.DataSource;

public class DaoManager {
	private static SubscriberDao SUBSCRIBER_DAO_INSTANCE;
	private static SituationDataFilterDao SITUATION_DATA_FILTER_DAO_INSTANCE;
	
	private static class DaoManagerHolder { 
		private static final DaoManager INSTANCE = new DaoManager();
	}
 
	public static DaoManager getInstance() {
		return DaoManagerHolder.INSTANCE;
	}
	
	private DataSource dataSource;
	private String subscriberTableName;
	private String filterTableName;
	
	private DaoManager() {
		// Prevents instantiation from other classes
	}
	
	public synchronized void setAndInitialize(
			DataSource dataSource, 
			String subscriberTableName, 
			String filterTableName) {
		if (SUBSCRIBER_DAO_INSTANCE == null && SITUATION_DATA_FILTER_DAO_INSTANCE == null) {
			this.dataSource = dataSource;
			this.subscriberTableName = subscriberTableName;
			this.filterTableName = filterTableName;
			
			SubscriberDao.Builder subcriberDaoBuilder = new SubscriberDao.Builder();
			subcriberDaoBuilder.setDataSource(this.dataSource).setTableName(this.subscriberTableName);
			SUBSCRIBER_DAO_INSTANCE = subcriberDaoBuilder.build();
			
			SituationDataFilterDao.Builder filterDaoBuilder = new SituationDataFilterDao.Builder();
			filterDaoBuilder.setDataSource(this.dataSource).setTableName(this.filterTableName);
			SITUATION_DATA_FILTER_DAO_INSTANCE = filterDaoBuilder.build();
		}
	}
	
	public SubscriberDao getSubscriberDao() {
		return SUBSCRIBER_DAO_INSTANCE;
	}
	
	public SituationDataFilterDao getSituationDataFilterDao() {
		return SITUATION_DATA_FILTER_DAO_INSTANCE;
	}
}