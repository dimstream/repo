package com.vmware.conn;

import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.j256.ormlite.db.DatabaseType;
import com.j256.ormlite.db.DerbyClientServerDatabaseType;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.TableUtils;
import com.vmware.common.conf.AppConfig;
import com.vmware.common.dao.Action;
import com.vmware.common.dao.ComputedStates;
import com.vmware.common.dao.ConfigurationDetails;
import com.vmware.common.dao.ConfigurationProperties;
import com.vmware.common.dao.ContextMapping;
import com.vmware.common.dao.EventStream;
import com.vmware.common.dao.ExternalLookUp;
import com.vmware.common.dao.Identifier;
import com.vmware.common.dao.PersistedFields;
import com.vmware.common.dao.ProcessContext;
import com.vmware.common.dao.RevisedFields;
import com.vmware.common.dao.Trap;
import com.vmware.common.dao.TrapActionConfig;
import com.vmware.common.dao.URLTag;

/**
 * Class for Managing the Derby Database Connection
 * 
 * @author ghimanshu
 *
 */
public class DBConnection {

	static ConnectionSource conn = null;
	private static final Logger logger = LogManager.getLogger(DBConnection.class);

	/**
	 * Method to retrieve the DB Connection
	 * 
	 * @return Connection Object
	 * @throws SQLException SQLException
	 */
	public static ConnectionSource getConnection() throws SQLException {
		logger.traceEntry();
		if (conn != null)
			return conn;
		return logger.traceExit(connect());
	}

	private static ConnectionSource connect() throws SQLException {
		logger.traceEntry();
		DatabaseType databaseType = new DerbyClientServerDatabaseType();
		ConnectionSource connectionSource = new JdbcConnectionSource(AppConfig.connectionURL, databaseType);
		/*DatabaseType databaseType = new PostgresDatabaseType();
		ConnectionSource connectionSource = new JdbcConnectionSource(AppConfig.derbyUrl,"postgres","root", databaseType);*/
		return logger.traceExit(connectionSource);
	}

	/**
	 * Method to create the tables, if it doesn't exists
	 * 
	 * @throws SQLException SQLException
	 */
	public static void initializeDB() throws SQLException {
		logger.traceEntry();
		ConnectionSource connectionSource = connect();
		try {
			logger.debug("DBConnection.initializeDB() ---  Creating Tables... ");
			TableUtils.createTableIfNotExists(connectionSource, EventStream.class);
			TableUtils.createTableIfNotExists(connectionSource, Identifier.class);
			TableUtils.createTableIfNotExists(connectionSource, ProcessContext.class);
			TableUtils.createTableIfNotExists(connectionSource, ComputedStates.class);
			TableUtils.createTableIfNotExists(connectionSource, PersistedFields.class);
			TableUtils.createTableIfNotExists(connectionSource, RevisedFields.class);
			TableUtils.createTableIfNotExists(connectionSource, ContextMapping.class);
			TableUtils.createTableIfNotExists(connectionSource, ExternalLookUp.class);
			TableUtils.createTableIfNotExists(connectionSource, URLTag.class);
			TableUtils.createTableIfNotExists(connectionSource, ConfigurationDetails.class);
			TableUtils.createTableIfNotExists(connectionSource, ConfigurationProperties.class);
			TableUtils.createTableIfNotExists(connectionSource, TrapActionConfig.class);
			TableUtils.createTableIfNotExists(connectionSource, Action.class);
			TableUtils.createTableIfNotExists(connectionSource, Trap.class);

		} catch (SQLException sqlException) {
			connectionSource.close();
		}
		logger.traceExit();
	}
}