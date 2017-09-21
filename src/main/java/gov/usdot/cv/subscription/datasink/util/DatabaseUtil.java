package gov.usdot.cv.subscription.datasink.util;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;

import com.deleidos.rtws.commons.dao.jdbc.DataAccessSession;
import com.deleidos.rtws.commons.dao.jdbc.DataAccessUtil;
import com.deleidos.rtws.commons.exception.InitializationException;

public class DatabaseUtil {
	private DatabaseUtil() { 
		// Uses static methods for all invocation.
	}
	
	public static synchronized void buildSubscriptionTables(
			DataSource dataSource, 
			String subscriberTableName, 
			String filterTableName) {
		DataAccessSession session = DataAccessUtil.session(dataSource);
		
		try {
			PreparedStatement createSchemaStmt = session.prepareStatement("CREATE SCHEMA IF NOT EXISTS APPLICATION;");
			session.executeStatement(createSchemaStmt, null);
		} catch (SQLException sqle) {
			throw new InitializationException("Failed to create application schema.", sqle);
		}
		
		try {
			StringBuilder createSubscriberTableStmt = new StringBuilder();
			if (StringUtils.isEmpty(subscriberTableName)) {
				createSubscriberTableStmt.append("CREATE TABLE IF NOT EXISTS APPLICATION.SUBSCRIBER(");
			} else {
				createSubscriberTableStmt.append("CREATE TABLE IF NOT EXISTS APPLICATION." + subscriberTableName.trim() + "(");
			}
			createSubscriberTableStmt.append("ID 			INT 			NOT NULL PRIMARY KEY,");
			createSubscriberTableStmt.append("CERTIFICATE 	BINARY 			NOT NULL,");
			createSubscriberTableStmt.append("TARGET_HOST 	VARCHAR(255) 	NOT NULL,");
			createSubscriberTableStmt.append("TARGET_PORT 	INT 			NOT NULL,");
			createSubscriberTableStmt.append(");");
			
			PreparedStatement createUserTablePreparedStmt = session.prepareStatement(createSubscriberTableStmt.toString());
			session.executeStatement(createUserTablePreparedStmt, null);
		} catch (SQLException sqle) {
			throw new InitializationException("Failed to create subscription user table.", sqle);
		}
		
		try {
			StringBuilder createFilterTableStmt = new StringBuilder();
			if (StringUtils.isEmpty(filterTableName)) {
				createFilterTableStmt.append("CREATE TABLE IF NOT EXISTS APPLICATION.SITUATION_DATA_FILTER(");
			} else {
				createFilterTableStmt.append("CREATE TABLE IF NOT EXISTS APPLICATION." + filterTableName.trim() + "(");
			}
			createFilterTableStmt.append("ID 			INT 		NOT NULL,");
			createFilterTableStmt.append("END_TIME 		TIMESTAMP 	NOT NULL,");
			createFilterTableStmt.append("TYPE 			VARCHAR(32) NOT NULL,");
			createFilterTableStmt.append("TYPE_VALUE 	INT 		NOT NULL,");
			createFilterTableStmt.append("REQUEST_ID 	INT 		NOT NULL,");
			createFilterTableStmt.append("NW_LAT 		NUMBER,");
			createFilterTableStmt.append("NW_LON 		NUMBER,");
			createFilterTableStmt.append("SE_LAT 		NUMBER,");
			createFilterTableStmt.append("SE_LON 		NUMBER,");
			if (StringUtils.isEmpty(filterTableName)) {
				createFilterTableStmt.append("CONSTRAINT SUBSCRIBER_ID_FK FOREIGN KEY(ID) REFERENCES APPLICATION.SUBSCRIBER(ID)");
			} else {
				createFilterTableStmt.append("CONSTRAINT " + filterTableName.trim() + "_ID_FK FOREIGN KEY(ID) REFERENCES APPLICATION." + subscriberTableName.trim() + "(ID)");
			}
			createFilterTableStmt.append(");");
			
			PreparedStatement createFilterTablePreparedStmt = session.prepareStatement(createFilterTableStmt.toString());
			session.executeStatement(createFilterTablePreparedStmt, null);
		} catch (SQLException sqle) {
			throw new InitializationException("Failed to create situation data filter table.", sqle);
		}
	}
}