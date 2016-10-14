/*
 * Copyright 2016 Saxon State and University Library Dresden (SLUB)
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

import de.qucosa.fedora.mets.ReportingDocumentMetadata;
import de.qucosa.fedora.oai.OaiHeader;
import de.qucosa.fedora.oai.OaiRunResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class PostgrePersistenceService implements PersistenceService {

    private final String databasePassword;
    private final String databaseUser;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String url;

    /**
     * @param url              as required by
     *                         {@link DriverManager#getConnection(String, String, String)}
     * @param databaseUser     as required by
     *                         {@link DriverManager#getConnection(String, String, String)}
     * @param databasePassword as required by
     *                         {@link DriverManager#getConnection(String, String, String)}
     * @throws IllegalArgumentException if any parameter is {@code null}
     * @throws SQLException    in case credentials can't be used to extablish a database connection
     * to the url or a database access error occurs 
     */
    public PostgrePersistenceService(String url, String databaseUser,
                                     String databasePassword) throws IllegalArgumentException, SQLException {
        if (url == null) {
            throw new IllegalArgumentException("parameter url must not be null");
        }
        if (databaseUser == null) {
            throw new IllegalArgumentException("parameter databaseUser must not be null");
        }
        if (databasePassword == null) {
            throw new IllegalArgumentException("parameter databasePassword must not be null");
        }

        this.url = url;
        this.databaseUser = databaseUser;
        this.databasePassword = databasePassword;

        // check url and credentials and throw SQLException if a database access error occurs
        Connection con = DriverManager.getConnection(url, databaseUser, databasePassword);
        con.close();
    }

    /*
     * (non-Javadoc)
     * 
     * @see de.qucosa.persistence.PersistenceService#getLastOaiRunResult()
     */
    @Override
//    @Nullable
    public OaiRunResult getLastOaiRunResult() {

        OaiRunResult oaiRunResult = null;

        String errorMsg = "Could not fetch OAI run result data from database. "
                + "Returning the default (null) as if there was no OAI run result in the database. ";

        String stm = "SELECT \"timestampOfRun\", \"responseDate\", \"resumptionToken\", \"resumptionTokenExpirationDate\", \"nextFromTimestamp\" FROM \"OAIRunResult\" order by \"ID\" desc limit 1";

        try (Connection con = DriverManager.getConnection(url, databaseUser, databasePassword);
             PreparedStatement pst = con.prepareStatement(stm);
             ResultSet rs = pst.executeQuery();) {

            int rowCount = 0;
            while (rs.next()) {
                ++rowCount;

                Date lastRun = convertNullableSQLTimestampToJavaDate(rs.getTimestamp("timestampOfRun"));
                if (lastRun == null) {
                    logger.error(errorMsg + "Error: 'timestampOfRun' must not be null.");
                    break;
                }

                Date responseDate = convertNullableSQLTimestampToJavaDate(rs.getTimestamp("responseDate"));
                if (responseDate == null) {
                    logger.error(errorMsg + "Error: 'responseDate' must not be null.");
                    break;
                }

                oaiRunResult = new OaiRunResult(lastRun, responseDate, rs.getString("resumptionToken"),
                        convertNullableSQLTimestampToJavaDate(rs.getTimestamp("resumptionTokenExpirationDate")),
                        convertNullableSQLTimestampToJavaDate(rs.getTimestamp("nextFromTimestamp")));

            }

            if (rowCount == 0) {
                logger.debug("Could not fetch any OAI run result from database.");
            }

        } catch (SQLException e) {
            // TODO do exception handling here??
            logger.error(errorMsg + "Exception details:", e);
        }

        return oaiRunResult;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * de.qucosa.persistence.PersistenceService#storeOaiRunResult(de.qucosa.fedora.
     * oai.OaiRunResult)
     */
    @Override
    public void storeOaiRunResult(OaiRunResult oaiRunResult) throws PersistenceException {

        //TODO check oaiRunResult == null; throw NPE or PersistenceException?
        
        String insertStm = "INSERT INTO \"OAIRunResult\"(\"timestampOfRun\", \"responseDate\", \"resumptionToken\", \"resumptionTokenExpirationDate\", \"nextFromTimestamp\") VALUES(?, ?, ?, ?, ?)";

        try (Connection con = DriverManager.getConnection(url, databaseUser, databasePassword);
             PreparedStatement pst = con.prepareStatement(insertStm)) {

            pst.setTimestamp(1, convertNullableJAVADateToSQLTimestamp(oaiRunResult.getTimestampOfRun()));
            pst.setTimestamp(2, convertNullableJAVADateToSQLTimestamp(oaiRunResult.getResponseDate()));
            pst.setString(3, oaiRunResult.getResumptionToken());
            pst.setTimestamp(4, convertNullableJAVADateToSQLTimestamp(oaiRunResult.getResumptionTokenExpirationDate()));
            pst.setTimestamp(5, convertNullableJAVADateToSQLTimestamp(oaiRunResult.getNextFromTimestamp()));
            pst.executeUpdate();

        } catch (SQLException e) {

            throw new PersistenceException("Could not store OaiRunResult in database.", e);
        }
    }

    @Override
    public void cleanupOaiRunResults(Date oldestResultToKeep) throws PersistenceException {

        //TODO check oldestResultToKeep == null; throw NPE or PersistenceException?
        
        Integer lastOaiRunResultIDtoKeep = null;
        String getID = "SELECT \"ID\" FROM \"OAIRunResult\" order by \"ID\" desc limit 1";

        try (Connection con = DriverManager.getConnection(url, databaseUser, databasePassword);
             PreparedStatement pst = con.prepareStatement(getID);
             ResultSet rs = pst.executeQuery();) {

            int rowCount = 0;
            while (rs.next()) {
                ++rowCount;
                lastOaiRunResultIDtoKeep = rs.getInt("ID");
            }
            if (rowCount == 0) {
                logger.debug("Could not fetch any OAI run result from database.");
            }

        } catch (SQLException e) {

            throw new PersistenceException("Could not fetch OAI run result to keep from database.", e);
        }

        //
        if (lastOaiRunResultIDtoKeep != null) {

            String deleteHistory = "DELETE FROM \"OAIRunResult\" WHERE \"timestampOfRun\" <= ? AND \"ID\" != ?";

            try (Connection con = DriverManager.getConnection(url, databaseUser, databasePassword);
                 PreparedStatement pst = con.prepareStatement(deleteHistory)) {

                pst.setTimestamp(1, convertNullableJAVADateToSQLTimestamp(oldestResultToKeep));
                pst.setInt(2, lastOaiRunResultIDtoKeep);
                int result = pst.executeUpdate();

                logger.debug("Number of deleted OaiRunResults: " + result);

            } catch (SQLException e) {
                throw new PersistenceException("Could not delete OaiRunResults from database.", e);
            }

        }
    }

    @Override
    public void addOrUpdateOaiHeaders(List<OaiHeader> headers) throws PersistenceException {

        //TODO check headers == null; throw NPE or PersistenceException?
        
        String basicErrorMsg = "Could not store all OaiHeaders in database. ";
        String stm = "INSERT INTO \"OAIHeader\" (\"recordIdentifier\", \"datestamp\" , \"setSpec\", \"statusIsDeleted\") VALUES (?, ?, ?, ?) ON CONFLICT (\"recordIdentifier\") DO UPDATE SET \"datestamp\" = ?, \"setSpec\" = ?, \"statusIsDeleted\" = ?";
        int[] results = {};

        try (Connection con = DriverManager.getConnection(url, databaseUser, databasePassword);
             PreparedStatement pst = con.prepareStatement(stm)) {

            con.setAutoCommit(false);

            for (OaiHeader header : headers) {

                pst.setString(1, header.getRecordIdentifier());
                Timestamp datestamp = convertNullableJAVADateToSQLTimestamp(header.getDatestamp());
                pst.setTimestamp(2, datestamp);

                Array setSpecArray = con.createArrayOf("varchar", header.getSetSpec().toArray());
                pst.setArray(3, setSpecArray);

                pst.setBoolean(4, header.isStatusIsDeleted());
                pst.setTimestamp(5, datestamp);

                pst.setArray(6, setSpecArray);

                pst.setBoolean(7, header.isStatusIsDeleted());
                pst.addBatch();

            }

            results = pst.executeBatch();
            con.commit();

        } catch (SQLException e) {
            throw new PersistenceException(basicErrorMsg, e);
        }

        StringBuilder resultError = new StringBuilder();
        boolean allUpdatesSuccess = true;

        for (int index = 0; index < results.length; index++) {
            int singleResult = results[index];

            if ((singleResult < 0) && (singleResult != Statement.SUCCESS_NO_INFO)) {
                resultError.append("Could not insert or update '").append(headers.get(index)).append("'. ");
                allUpdatesSuccess = false;

            } else if (singleResult > 1) {
                resultError.append(singleResult)
                        .append(" OaiHeaders have been added or modified when trying to add/modify one single header '")
                        .append(headers.get(index)).append("'. Database may be corrupted! ");
                allUpdatesSuccess = false;
            }
        }
        if (!allUpdatesSuccess) {
            
            //TODO @Ralf: should we rollback if there were items not persisted?
            throw new PersistenceException(basicErrorMsg + resultError);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see de.qucosa.persistence.PersistenceService#getOaiHeaders()
     */
    @Override
    public List<OaiHeader> getOaiHeaders() throws PersistenceException {
        List<OaiHeader> headers = new LinkedList<>();

        String stm = "SELECT \"recordIdentifier\", \"datestamp\" , \"setSpec\", \"statusIsDeleted\" from \"OAIHeader\" LIMIT 100";

        try (Connection con = DriverManager.getConnection(url, databaseUser, databasePassword);
             PreparedStatement pst = con.prepareStatement(stm);
             ResultSet rs = pst.executeQuery();) {

            int rowCount = 0;
            while (rs.next()) {
                ++rowCount;

                String recordIdentifier = rs.getString("recordIdentifier");
                if (recordIdentifier == null) {
                    logger.error("'recordIdentifier' must not be null. Skipping this OaiHeader.");
                    continue;
                }

                Date datestamp = convertNullableSQLTimestampToJavaDate(rs.getTimestamp("datestamp"));
                if (datestamp == null) {
                    logger.error("'datestamp' must not be null. Skipping OaiHeader with recordIdentifier '{}'",
                            recordIdentifier);
                    continue;
                }

                Array z = rs.getArray("setSpec");
                List<String> setSpec = new LinkedList<>();
                if (z != null) {
                    String[] setSpecArray = (String[]) z.getArray();
                    for (String element : setSpecArray) {
                        setSpec.add(element);
                    }
                }

                OaiHeader actualHeader = new OaiHeader(recordIdentifier, datestamp, setSpec,
                        rs.getBoolean("statusIsDeleted"));
                headers.add(actualHeader);

            }

            if (rowCount == 0) {
                logger.debug("There are currently no OaiHeaders in database.");
            }

        } catch (SQLException e) {
            throw new PersistenceException("Could not fetch OaiHeaders from database.", e);
        }

        return headers;
    }
    

    /*
     * (non-Javadoc)
     * 
     * @see de.qucosa.persistence.PersistenceService#getOaiHeaders()
     */
    @Override
    public List<OaiHeader> removeOaiHeadersIfUnmodified(List<OaiHeader> headersToRemove)
            throws PersistenceException {
        
        //TODO check headersToRemove == null; throw NPE or PersistenceException?

        // delete statements only if they did not change since we read them from
        // database
        String stm = "DELETE FROM \"OAIHeader\" WHERE \"recordIdentifier\" = ? AND \"datestamp\" = ? AND \"statusIsDeleted\" = ?";

        int[] results = {};

        try (Connection con = DriverManager.getConnection(url, databaseUser, databasePassword);
             PreparedStatement pst = con.prepareStatement(stm)) {

            con.setAutoCommit(false);

            for (OaiHeader header : headersToRemove) {

                pst.setString(1, header.getRecordIdentifier());
                Timestamp datestamp = convertNullableJAVADateToSQLTimestamp(header.getDatestamp());
                pst.setTimestamp(2, datestamp);
                pst.setBoolean(3, header.isStatusIsDeleted());
                pst.addBatch();

            }

            results = pst.executeBatch();
            con.commit();

        } catch (SQLException e) {
            throw new PersistenceException("Could not remove any OaiHeader.", e);
        }

        StringBuilder exceptionMsg = new StringBuilder();
        List<OaiHeader> headersNotRemoved = new LinkedList<>();

        for (int index = 0; index < results.length; index++) {
            int singleResult = results[index];
            if (singleResult == 0) {

                headersNotRemoved.add(headersToRemove.get(index));
            } else if (singleResult > 1) {

                exceptionMsg.append(singleResult)
                        .append(" OaiHeaders have been deleted when trying to delete the single header '")
                        .append(headersToRemove.get(index)).append("'. Database may be corrupted!");

                // logger.error("{} OaiHeaders have been deleted when trying to
                // delete the single header {}. "
                // + "Database may be corrupted!", singleResult,
                // headersToRemove.get(index));
            }
        }
        if (!headersNotRemoved.isEmpty()) {
            logger.debug("Did not remove all OaiHeaders. Maybe they have been updated "
                    + "since they had been loaded from database. Items not removed: " + headersNotRemoved);
        }

        if (exceptionMsg.length() > 0) {
            // FIXME @Ralf: should we throw an exception here? It hides the
            // return value. on the other hand,
            //
            throw new PersistenceException(exceptionMsg.toString());
        }

        return headersNotRemoved;
    }
    
      
    

    /*
     * (non-Javadoc)
     * 
     * @see de.qucosa.persistence.PersistenceService#getOaiHeaders()
     */
    @Override
    public void addOrUpdateReportingDocuments(List<ReportingDocumentMetadata> reportingDocuments)
            throws PersistenceException {

        //TODO check reportingDocuments == null; throw NPE or PersistenceException?
        
        String basicErrorMsg = "Could not store all OaiHeaders in database. ";
        String stm = "INSERT INTO \"ReportingDocuments\" (\"recordIdentifier\", \"mandator\" , \"documentType\", \"distributionDate\", \"headerLastModified\") VALUES (?, ?, ?, ?, ?) ON CONFLICT (\"recordIdentifier\") DO UPDATE SET \"mandator\" = ?, \"documentType\" = ?, \"distributionDate\" = ?, \"headerLastModified\" = ?";
        int[] results = {};

        try (Connection con = DriverManager.getConnection(url, databaseUser, databasePassword);
             PreparedStatement pst = con.prepareStatement(stm)) {

            con.setAutoCommit(false);

            for (ReportingDocumentMetadata reportingDoc : reportingDocuments) {

                pst.setString(1, reportingDoc.getRecordIdentifier());

                pst.setString(2, reportingDoc.getMandator());

                pst.setString(3, reportingDoc.getDocumentType());

                Timestamp distributionDate = convertNullableJAVADateToSQLTimestamp(reportingDoc.getDistributionDate());
                pst.setTimestamp(4, distributionDate);

                Timestamp headerLastModified = convertNullableJAVADateToSQLTimestamp(reportingDoc.getHeaderLastModified());
                pst.setTimestamp(5, headerLastModified);

                pst.setString(6, reportingDoc.getMandator());

                pst.setString(7, reportingDoc.getDocumentType());

                pst.setTimestamp(8, distributionDate);

                pst.setTimestamp(9, headerLastModified);

                pst.addBatch();
            }

            results = pst.executeBatch();
            con.commit();

        } catch (SQLException e) {
            throw new PersistenceException(basicErrorMsg, e);
        }

        StringBuilder resultError = new StringBuilder();
        boolean allUpdatesSuccess = true;

        for (int index = 0; index < results.length; index++) {
            int singleResult = results[index];

            if ((singleResult < 0) && (singleResult != Statement.SUCCESS_NO_INFO)) {
                resultError.append("Could not insert or update '").append(reportingDocuments.get(index)).append("'. ");
                allUpdatesSuccess = false;

            } else if (singleResult > 1) {
                resultError.append(singleResult)
                        .append(" reportingDocuments have been added or modified when trying to add/modify one single item '")
                        .append(reportingDocuments.get(index)).append("'. Database may be corrupted! ");
                allUpdatesSuccess = false;
            }
        }
        if (!allUpdatesSuccess) {
            
            //TODO @Ralf: should we rollback if there were items not persisted or updated?
            throw new PersistenceException(basicErrorMsg + resultError);
        }
    }

    /**
     * @param date the {@link java.util.Date} to convert or {@code null}
     * @return {@link java.sql.Timestamp} the converted value or {@code null} if
     * argument date was {@code null}
     */
//    @Nullable
    private Timestamp convertNullableJAVADateToSQLTimestamp(Date date) {
        Timestamp sqlTimestamp = null;

        if (date != null) {
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            sqlTimestamp = new java.sql.Timestamp(cal.getTimeInMillis());
        }
        return sqlTimestamp;
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
            if (microsAndNanos != 0) {
                logger.warn("Loosing precision of " + microsAndNanos
                        + " nanoseconds while creating a new java.util.Date object from java.sql.Timestamp '"
                        + timestamp + "'");
            }

            Calendar cal = Calendar.getInstance();
            cal.setTime(timestamp);
            date = cal.getTime();
        }

        return date;
    }

}
