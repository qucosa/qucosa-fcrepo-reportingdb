/*
 * Copyright 2016 SLUB Dresden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.slub.persistence;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.commons.io.IOUtils;

import de.slub.fedora.oai.OaiHarvesterTestIT;

/**
 * Helper class for integration tests
 */
public class PostgrePersistenceServiceTestHelper {

    private final String DATABASE_URL;
    private final String DATABASE_USER;
    private final String DATABASE_PASSWORD;

    /**
     * Use {@link #getConnection()} to access this {@link Connection}!
     */
    private Connection connection = null;

    public PostgrePersistenceServiceTestHelper(String url, String databaseUser, String databasePassword) {
        this.DATABASE_URL = url;
        this.DATABASE_USER = databaseUser;
        this.DATABASE_PASSWORD = databasePassword;
    }

    public void executeQueriesFromFile(String filePath) throws Exception {
        String sqlQuery = "";
        try (InputStream resourceAsStream = OaiHarvesterTestIT.class.getResourceAsStream(filePath);) {
            sqlQuery = IOUtils.toString(resourceAsStream, "UTF-8");
        }

        try (PreparedStatement pst = getConnection().prepareStatement(sqlQuery)) {
            pst.execute();
        }
    }

    public int countOaiRunResults() throws Exception {

        int count = 0;
        String stm = "SELECT COUNT(*) FROM \"OAIRunResult\"";

        try (PreparedStatement pst = getConnection().prepareStatement(stm); ResultSet rs = pst.executeQuery();) {

            while (rs.next()) {
                count = rs.getInt(1);
            }
        }
        return count;
    }

    /**
     * Establishes the connection to the test database if there is no valid
     * connection yet and return it.
     * 
     * @return connection to test database, never {@code null}
     * @throws Exception
     */
    private Connection getConnection() throws Exception {
        if (connection == null || !connection.isValid(2)) { // 2 seconds timeout
            // TODO do we need to close the Connection in unit test code?
            // -> yes, should be closed in a tearDown() after all tests are
            // done.
            connection = DriverManager.getConnection(DATABASE_URL, DATABASE_USER, DATABASE_PASSWORD);
        }
        return connection;
    }

}
