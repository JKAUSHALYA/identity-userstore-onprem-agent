/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.carbon.identity.user.store.outbound.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.identity.user.store.outbound.exception.WSUserStoreException;

import java.sql.Connection;
import java.sql.SQLException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

public class DatabaseUtil {

    private static Log LOGGER = LogFactory.getLog(DatabaseUtil.class);
    private DataSource dataSource;
    private static final String DATA_SOURCE_NAME = "jdbc/TokenDB";
    private static volatile DatabaseUtil instance = new DatabaseUtil();

    private DatabaseUtil() {
        initDataSource();
    }

    public static DatabaseUtil getInstance() {
        return instance;
    }

    /**
     * Initialize datasource
     */
    private void initDataSource() {

        try {
            Context ctx = new InitialContext();
            dataSource = (DataSource) ctx.lookup(DATA_SOURCE_NAME);
        } catch (NamingException e) {
            String errorMsg = "Error when looking up the Identity Data Source.";
            LOGGER.error(errorMsg, e);
        }
    }

    /**
     * Returns an database connection.
     *
     * @return dbConnection
     * @throws WSUserStoreException
     * @Deprecated The getDBConnection should handle both transaction and non-transaction connection. Earlier it
     * handle only the transactionConnection. Therefore this method was deprecated and changed as handle both
     * transaction and non-transaction connection. getDBConnection(boolean shouldApplyTransaction) method used as
     * alternative of this method.
     */
    @Deprecated
    public Connection getDBConnection() throws WSUserStoreException {

        return getDBConnection(true);
    }

    /**
     * Get database connection
     * @return database connection
     * @throws WSUserStoreException
     */
    public Connection getDBConnection(boolean shouldApplyTransaction) throws WSUserStoreException {

        try {
            if (dataSource == null) {
                throw new WSUserStoreException("Error occurred while getting connection. Datasource is null");
            }
            Connection dbConnection = dataSource.getConnection();
            if (shouldApplyTransaction) {
                dbConnection.setAutoCommit(false);
                dbConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            }
            return dbConnection;
        } catch (SQLException e) {
            String errMsg = "Error when getting a database connection object from the Identity data source.";
            throw new WSUserStoreException(errMsg, e);
        }
    }

    /**
     * Revoke the transaction when catch then sql transaction errors.
     *
     * @param dbConnection database connection.
     * @throws SQLException SQL Exception.
     */
    public static void rollbackTransaction(Connection dbConnection) {

        try {
            if (dbConnection != null) {
                dbConnection.rollback();
            }
        } catch (SQLException e1) {
            LOGGER.error("An error occurred while rolling back transactions. ", e1);
        }
    }

    /**
     * Commit the transaction.
     *
     * @param dbConnection database connection.
     * @throws SQLException SQL Exception.
     */
    public static void commitTransaction(Connection dbConnection) {

        try {
            if (dbConnection != null) {
                dbConnection.commit();
            }
        } catch (SQLException e1) {
            LOGGER.error("An error occurred while commit transactions. ", e1);
        }
    }
}
