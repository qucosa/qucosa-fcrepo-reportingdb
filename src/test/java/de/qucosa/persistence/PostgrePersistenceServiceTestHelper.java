/*
 * Copyright 2017 Saxon State and University Library Dresden (SLUB)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.qucosa.persistence;

import java.io.InputStream;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.IOUtils;

import de.qucosa.fedora.mets.ReportingDocumentMetadata;
import de.qucosa.fedora.oai.OaiHarvesterTest;
import de.qucosa.fedora.oai.OaiHeader;

/**
 * Helper class for integration tests
 */
public class PostgrePersistenceServiceTestHelper {

    private final String DATABASE_PASSWORD;
    private final String DATABASE_URL;
    private final String DATABASE_USER;
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
        String sqlQuery;
        try (InputStream resourceAsStream = OaiHarvesterTest.class.getResourceAsStream(filePath)) {
            sqlQuery = IOUtils.toString(resourceAsStream, "UTF-8");
        }

        try (PreparedStatement pst = getConnection().prepareStatement(sqlQuery)) {
            pst.execute();
        }
    }

    public int countOaiRunResults() throws Exception {

        int count = 0;
        String stm = "SELECT COUNT(*) FROM \"OAIRunResult\"";

        try (PreparedStatement pst = getConnection().prepareStatement(stm); ResultSet rs = pst.executeQuery()) {

            while (rs.next()) {
                count = rs.getInt(1);
            }
        }
        return count;
    }
    
    
    public List<ReportingDocumentMetadata> getReportingDocumentMetadata() throws PersistenceException {
        List<ReportingDocumentMetadata> reportingDocuments = new LinkedList<>();

        String stm = "SELECT \"recordIdentifier\", \"mandator\" , \"documentType\", \"distributionDate\", \"headerLastModified\" from \"ReportingDocuments\" LIMIT 100";

        try (PreparedStatement pst = getConnection().prepareStatement(stm);
             ResultSet rs = pst.executeQuery();) {

            while (rs.next()) {
                String recordIdentifier = rs.getString("recordIdentifier");
                String mandator = rs.getString("mandator");
                String documentType = rs.getString("documentType");
                Date distributionDate = convertNullableSQLTimestampToJavaDate(rs.getTimestamp("distributionDate"));
                Date headerLastModified = convertNullableSQLTimestampToJavaDate(rs.getTimestamp("headerLastModified"));
                ReportingDocumentMetadata document = new ReportingDocumentMetadata(recordIdentifier, mandator, documentType, distributionDate, headerLastModified);

                reportingDocuments.add(document);

            }
        } catch (Exception e) {
            throw new PersistenceException("Could not fetch ReportingDocuments from database.", e);
        }

        return reportingDocuments;
    }



    /**
     * @param timestamp the {@link java.sql.Timestamp} to convert to or {@code null}
     * @return {@link java.util.Date} the converted value or {@code null} if
     * argument timestamp was {@code null}
     */
//    @Nullable
    private Date convertNullableSQLTimestampToJavaDate(Timestamp timestamp) {
        Date date = null;
        if (timestamp != null) {
            // do we lose precision? Date has milliseconds, Timestamp
            // nanoseconds
            int microsAndNanos = timestamp.getNanos() % 1000000;

            Calendar cal = Calendar.getInstance();
            cal.setTime(timestamp);
            date = cal.getTime();
        }

        return date;
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
            connection = DriverManager.getConnection(DATABASE_URL, DATABASE_USER, DATABASE_PASSWORD);
        }
        return connection;
    }

    public void tearDown() throws Exception {
        if (connection==null)
            return;
        
        Connection connectionToTearDown = getConnection();
        
        if (! connectionToTearDown.getAutoCommit())
            connectionToTearDown.commit(); //TODO do this just in case there is something to commit?        
        
        if (! connectionToTearDown.isClosed())
            connectionToTearDown.close();
    }

}
