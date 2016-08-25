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

import de.qucosa.fedora.oai.OaiHeader;
import de.qucosa.fedora.oai.OaiRunResult;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests of the {@link PostgrePersistenceService}'s API.
 */
public class PostgrePersistenceServiceTestIT {

    // TODO read from Properties file??
    private static final String DATABASE_URL = "jdbc:postgresql://localhost:5432/reportingUnitTest";
    private static final String DATABASE_USER = "reportingDBUnitTest";
    private static final String DATABASE_PASSWORD = "76Sp)qpH2D";

    private static final String CREATE_SEQUENCES_AND_TABLES_SQL = "/persistence/createSequencesAndTables.sql";
    private static final String TRUNCATE_TABLES_SQL = "/persistence/truncateTables.sql";
    private static final String INSERT_OAI_RUN_RESULTS_SQL = "/persistence/insertOAIRunResults.sql";
    private static final String INSERT_OAI_HEADERS_SQL = "/persistence/insertOaiHeaders.sql";

    private static PostgrePersistenceServiceTestHelper testPersistenceService;
    private PersistenceService persistenceService;

    /* ---- Begin OaiRunResult tests ---- */

    /**
     * Write several {@link OaiRunResult}s to database and assert that the last
     * written object is returned by
     * {@link PersistenceService#getLastOaiRunResult()}. The insertion is not
     * done by {@link PersistenceService} to test reading from database
     * independently.
     *
     * @throws Exception
     */
    @Test
    public void readLastRunResult() throws Exception {
        testPersistenceService.executeQueriesFromFile(INSERT_OAI_RUN_RESULTS_SQL);
        assertEquals("Wrong number of OaiRunResults in database, precondition of test failed!", 3,
                testPersistenceService.countOaiRunResults());

        OaiRunResult actual = persistenceService.getLastOaiRunResult();
        assertNotNull("Could not load any OaiRunResult from database", actual);

        // look for '2016-07-20 13:22:57.137+02', '2011-01-03 11:00:23-03',
        // '140225245500000', '2014-06-09 20:34:15+04'
        // and be aware of time zones
        SimpleDateFormat dateFormatLastRun = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        dateFormatLastRun.setTimeZone(TimeZone.getTimeZone("GMT+2:00"));
        Date lastRun = dateFormatLastRun.parse("2016-07-20 13:22:57.137+02");

        SimpleDateFormat dateFormatResponseDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormatResponseDate.setTimeZone(TimeZone.getTimeZone("GMT-3:00"));
        Date responseDate = dateFormatResponseDate.parse("2011-01-03 12:00:23-03");

        String token = "140225245500000";

        SimpleDateFormat dateFormatTokenExpiration = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormatTokenExpiration.setTimeZone(TimeZone.getTimeZone("GMT+4:00"));
        Date tokenExpiration = dateFormatTokenExpiration.parse("2014-06-09 20:34:15+04");

        SimpleDateFormat dateFormatNextFromValue = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        dateFormatNextFromValue.setTimeZone(TimeZone.getTimeZone("GMT+2:00"));
        Date nextFromValue = dateFormatNextFromValue.parse("2016-07-20 13:18:40.038+02");

        OaiRunResult expected = new OaiRunResult(lastRun, responseDate, token, tokenExpiration, nextFromValue);

        assertEquals(expected, actual);
    }

    /**
     * Assert that a {@link OaiRunResult} can be written to and read from
     * database using {@link PostgrePersistenceService}.
     *
     * @throws Exception
     */
    @Test
    public void writeAndReadLastRunResult() throws Exception {

        Date expectedLastRun = DatatypeConverter.parseDateTime("2016-07-20T11:22:57Z").getTime();
        Date expectedResponseDate = DatatypeConverter.parseDateTime("2016-07-20T11:22:58Z").getTime();
        String expectedResumptionToken = "140225245500000";
        Date expectedResumptionTokenExpiration = DatatypeConverter.parseDateTime("2016-07-20T11:32:58Z").getTime();
        Date expectednextFromValue = DatatypeConverter.parseDateTime("2016-07-20T11:12:57Z").getTime();
        OaiRunResult expectedOaiRunResult = new OaiRunResult(expectedLastRun, expectedResponseDate,
                expectedResumptionToken, expectedResumptionTokenExpiration, expectednextFromValue);

        // since OaiRunResult is immutable, it's save to pass the same object to
        // PersistenceService for storage and compare it to the result read from
        // PersistenceService
        persistenceService.storeOaiRunResult(expectedOaiRunResult);

        OaiRunResult actualOaiRunResult = persistenceService.getLastOaiRunResult();
        assertNotNull("Could not load OaiRunResult from database", actualOaiRunResult);

        assertEquals(expectedOaiRunResult, actualOaiRunResult);
    }

    /**
     * An "empty" {@link OaiRunResult} with
     * {@link OaiRunResult#getTimestampOfRun()} {@code == null} must not be
     * written to database.<br />
     * (It is also expected to get a message on the error log.)
     *
     * @throws Exception
     */
    @Test(expected = PersistenceException.class)
    public void doNotWriteEmptyLastRunResult() throws Exception {

        persistenceService.storeOaiRunResult(new OaiRunResult());
    }

    /**
     * Test the insertion order of {@link OaiRunResult}s. Insert two
     * {@link OaiRunResult}s that have the same
     * {@link OaiRunResult#getTimestampOfRun()}. Assert that the last inserted
     * {@link OaiRunResult} is returned by
     * {@link PersistenceService#getLastOaiRunResult()}.
     *
     * @throws Exception
     */
    @Test
    public void writeLastRunResultsWithEqualLastRunTimestamps() throws Exception {

        Date lastRun = DatatypeConverter.parseDateTime("2016-07-20T11:22:57Z").getTime();
        Date expectedNextFromValue = DatatypeConverter.parseDateTime("2016-07-20T11:12:57Z").getTime();

        Date responseDate_1 = DatatypeConverter.parseDateTime("2016-07-20T11:22:58Z").getTime();
        Date responseDate_2 = DatatypeConverter.parseDateTime("2014-11-11T06:25:55Z").getTime();
        String resumptionToken_1 = "140225245500000";
        String resumptionToken_2 = "123456789012345";
        Date resumptionTokenExpiration_1 = DatatypeConverter.parseDateTime("2016-07-20T12:22:58Z").getTime();
        Date resumptionTokenExpiration_2 = DatatypeConverter.parseDateTime("2014-11-11T07:25:55Z").getTime();

        OaiRunResult oaiRunResult_1 = new OaiRunResult(lastRun, responseDate_1, resumptionToken_1,
                resumptionTokenExpiration_1, expectedNextFromValue);
        OaiRunResult oaiRunResult_2 = new OaiRunResult(lastRun, responseDate_2, resumptionToken_2,
                resumptionTokenExpiration_2, expectedNextFromValue);

        persistenceService.storeOaiRunResult(oaiRunResult_1);
        persistenceService.storeOaiRunResult(oaiRunResult_2);

        OaiRunResult actualOaiRunResult = persistenceService.getLastOaiRunResult();
        assertNotNull("Could not load any OaiRunResult from database", actualOaiRunResult);

        assertEquals(oaiRunResult_2, actualOaiRunResult);
    }

    /**
     * Test the insertion order of {@link OaiRunResult}s. Insert two
     * {@link OaiRunResult}s. The secondly inserted object's
     * {@link OaiRunResult#getTimestampOfRun()} is in the past of the object
     * inserted first. Assert that the last inserted {@link OaiRunResult} is
     * returned by {@link PersistenceService#getLastOaiRunResult()}.
     *
     * @throws Exception
     */
    @Test
    public void writeLastRunResultsWithIncorrectLastRunTimestampOrder() throws Exception {

        Date lastRun_1 = DatatypeConverter.parseDateTime("2016-07-20T11:22:57Z").getTime();
        Date lastRun_2 = DatatypeConverter.parseDateTime("2014-11-11T06:25:54Z").getTime();

        Date responseDate_1 = DatatypeConverter.parseDateTime("2016-07-20T11:22:58Z").getTime();
        Date responseDate_2 = DatatypeConverter.parseDateTime("2014-11-11T06:25:55Z").getTime();
        String resumptionToken_1 = "140225245500000";
        String resumptionToken_2 = "123456789012345";
        Date resumptionTokenExpiration_1 = DatatypeConverter.parseDateTime("2016-07-20T12:22:58Z").getTime();
        Date resumptionTokenExpiration_2 = DatatypeConverter.parseDateTime("2014-11-11T07:25:55Z").getTime();
        Date expectedNextFromValue_1 = DatatypeConverter.parseDateTime("2016-07-20T11:12:57Z").getTime();
        Date expectedNextFromValue_2 = DatatypeConverter.parseDateTime("2014-11-11T06:15:54Z").getTime();

        OaiRunResult oaiRunResult_1 = new OaiRunResult(lastRun_1, responseDate_1, resumptionToken_1,
                resumptionTokenExpiration_1, expectedNextFromValue_1);
        OaiRunResult oaiRunResult_2 = new OaiRunResult(lastRun_2, responseDate_2, resumptionToken_2,
                resumptionTokenExpiration_2, expectedNextFromValue_2);

        persistenceService.storeOaiRunResult(oaiRunResult_1);
        persistenceService.storeOaiRunResult(oaiRunResult_2);

        OaiRunResult actualOaiRunResult = persistenceService.getLastOaiRunResult();
        assertNotNull("Could not load any OaiRunResult from database", actualOaiRunResult);

        assertEquals(oaiRunResult_2, actualOaiRunResult);
    }

    /**
     * Test the cleanup of OaiRunResults in persistence layer. Assert that the
     * last inserted statement is always kept, even if it is older than the
     * specified last result to keep.
     *
     * @throws Exception
     */
    @Test
    public void cleanupOaiRunResultHistoryAlwaysKeepLastResult() throws Exception {

        // write 3 OaiRunResults to persistence that are older than one day
        testPersistenceService.executeQueriesFromFile(INSERT_OAI_RUN_RESULTS_SQL);
        assertEquals("Wrong number of OaiRunResults in database, precondition of test failed!", 3,
                testPersistenceService.countOaiRunResults());

        // do cleanup
        SimpleDateFormat dateFormatLastRunToKeep = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormatLastRunToKeep.setTimeZone(TimeZone.getTimeZone("GMT+2:00"));
        Date lastRunToKeep = dateFormatLastRunToKeep.parse("2016-07-22 13:22:57.137+02");
        persistenceService.cleanupOaiRunResults(lastRunToKeep);

        // check that only one result is remaining in persistence layer
        assertEquals("Wrong number of OaiRunResults in persistence layer.", 1,
                testPersistenceService.countOaiRunResults());

        // make sure the OaiRunResult inserted last has not been deleted.
        // look for '2016-07-20 13:22:57.137+02', '2011-01-03
        // 11:00:23-03', '140225245500000', '2014-06-09 20:34:15+04',
        // '2016-07-20 13:18:40.038+02' and be
        // aware of time zones
        OaiRunResult actual = persistenceService.getLastOaiRunResult();

        SimpleDateFormat dateFormatLastRun = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        dateFormatLastRun.setTimeZone(TimeZone.getTimeZone("GMT+2:00"));
        Date lastRun = dateFormatLastRun.parse("2016-07-20 13:22:57.137+02");

        SimpleDateFormat dateFormatResponseDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormatResponseDate.setTimeZone(TimeZone.getTimeZone("GMT-3:00"));
        Date responseDate = dateFormatResponseDate.parse("2011-01-03 12:00:23-03");

        String token = "140225245500000";

        SimpleDateFormat dateFormatTokenExpiration = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormatTokenExpiration.setTimeZone(TimeZone.getTimeZone("GMT+4:00"));
        Date tokenExpiration = dateFormatTokenExpiration.parse("2014-06-09 20:34:15+04");

        SimpleDateFormat dateFormatNextFromTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        dateFormatNextFromTimestamp.setTimeZone(TimeZone.getTimeZone("GMT+2:00"));
        Date nextFromTimestamp = dateFormatNextFromTimestamp.parse("2016-07-20 13:18:40.038+02");

        OaiRunResult expected = new OaiRunResult(lastRun, responseDate, token, tokenExpiration, nextFromTimestamp);

        assertEquals(expected, actual);
    }

    /**
     * Test the cleanup of OaiRunResults in persistence layer. Assert that all
     * OaiRunResults are kept that are newer than the specified oldest
     * OaiRunResult to keep. The test simulates three old results to delete and
     * two new results to keep.<br />
     *
     * @throws Exception
     */
    @Test
    public void cleanupOaiRunResultHistoryTimestampFilterORIGINAL() throws Exception {

        // write 3 OaiRunResults to persistence that are older than one day
        testPersistenceService.executeQueriesFromFile(INSERT_OAI_RUN_RESULTS_SQL);

        // write a 4th result to persistence that must not be deleted
        OaiRunResult fourthOaiRunResult = new OaiRunResult(now(), now(), "", null, null);
        persistenceService.storeOaiRunResult(fourthOaiRunResult);

        // write a 5th result to persistence that must not be deleted
        Date timestampOfRun = now();
        Date responseDate = DatatypeConverter.parseDateTime("2014-06-08T11:43:00Z").getTime();
        String token = "111111111111111";
        Date expirationDate = DatatypeConverter.parseDateTime("2014-06-09T18:34:15Z").getTime();
        OaiRunResult fifthOaiRunResult = new OaiRunResult(timestampOfRun, responseDate, token, expirationDate,
                timestampOfRun);
        persistenceService.storeOaiRunResult(fifthOaiRunResult);

        assertEquals("Wrong number of OaiRunResults in persistence layer, precondition of test failed!", 5,
                testPersistenceService.countOaiRunResults());

        persistenceService
                .cleanupOaiRunResults(new Date(timestampOfRun.getTime() - Duration.standardDays(1).getMillis()));

        // check that only two results are remaining in persistence
        assertEquals("Wrong number of OaiRunResults in database.", 2, testPersistenceService.countOaiRunResults());

        // Make sure the 5th OaiRunResult is returned as the most recently
        // inserted.
        OaiRunResult actualOaiRunResult = persistenceService.getLastOaiRunResult();
        assertEquals(fifthOaiRunResult, actualOaiRunResult);

        // TODO assert, that the other OaiRunResult in database is the one that
        // had been inserted as number four
    }

    /* ---- End OaiRunResult tests ---- */
    /* ---- Begin OaiHeader tests ---- */

    /**
     * Write several {@link OaiHeader}s to database and assert that they can be
     * read by {@link PersistenceService#getOaiHeaders()}. The insertion is not
     * done by {@link PersistenceService} to test reading from database
     * independently.
     *
     * @throws Exception
     */
    @Test
    public void readOaiHeaders() throws Exception {
        testPersistenceService.executeQueriesFromFile(INSERT_OAI_HEADERS_SQL);

        List<OaiHeader> actualOaiHeaders = persistenceService.getOaiHeaders();
        assertEquals("Wrong number of OaiHeaders returned.", 2, actualOaiHeaders.size());

        SimpleDateFormat dateFormatDateStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormatDateStamp.setTimeZone(TimeZone.getTimeZone("GMT+2:00"));
        Date dateStamp_1 = dateFormatDateStamp.parse("2016-07-10 10:10:40+02");

        OaiHeader header_1 = new OaiHeader("oai:example.org:qucosa:47", dateStamp_1, false);
        assertTrue("OaiHeader with id 'oai:example.org:qucosa:47' is missing or does not equal the expected header.",
                actualOaiHeaders.contains(header_1));

        Date dateStamp_2 = dateFormatDateStamp.parse("2015-07-10 13:13:13+02");
        List<String> setSpec2 = new LinkedList<>();
        setSpec2.add("test");
        setSpec2.add("test,\" with separator and quotes");
        OaiHeader header_2 = new OaiHeader("oai:example.org:qucosa:199", dateStamp_2, setSpec2, true);
        assertTrue("OaiHeader with id 'oai:example.org:qucosa:199' is missing or does not equal the expected header.",
                actualOaiHeaders.contains(header_2));
    }

    /**
     * Write two {@link OaiHeader}s to database and read them.
     *
     * @throws Exception
     */
    @Test
    public void writeAndReadOaiHeaders() throws Exception {

        List<OaiHeader> expectedHeaders = new LinkedList<>();

        Date dateStamp_1 = DatatypeConverter.parseDateTime("2016-07-20T11:22:57Z").getTime();
        OaiHeader header_1 = new OaiHeader("oai:example.org:qucosa:123", dateStamp_1, false);
        expectedHeaders.add(header_1);

        Date dateStamp_2 = DatatypeConverter.parseDateTime("2012-03-30T06:54:12Z").getTime();
        List<String> setSpec2 = new LinkedList<>();
        setSpec2.add("test");
        setSpec2.add("test,\" with separator and quotes");
        OaiHeader header_2 = new OaiHeader("oai:example.org:qucosa:199", dateStamp_2, setSpec2, true);
        expectedHeaders.add(header_2);

        persistenceService.addOrUpdateHeaders(expectedHeaders);
        List<OaiHeader> actualHeaders = persistenceService.getOaiHeaders();

        assertEquals(expectedHeaders, actualHeaders);
    }

    /**
     * Write a {@link OaiHeader} to database. Modify its {@code dateStamp},
     * {@code setSpec} and {@code statusIsDeleted} and write it to database a
     * second time. The data set must have been updated in database.
     *
     * @throws Exception
     */
    @Test
    public void updateOaiHeader() throws Exception {

        List<OaiHeader> initialHeaders = new LinkedList<>();

        String recordIdentifier = "oai:example.org:qucosa:123";
        Date dateStamp_1 = DatatypeConverter.parseDateTime("2012-03-30T06:54:12Z").getTime();
        OaiHeader header_1 = new OaiHeader(recordIdentifier, dateStamp_1, false);
        initialHeaders.add(header_1);
        persistenceService.addOrUpdateHeaders(initialHeaders);

        List<OaiHeader> expectedHeaders = new LinkedList<>();
        Date dateStamp_2 = DatatypeConverter.parseDateTime("2016-07-20T11:22:57Z").getTime();

        List<String> setSpec2 = new LinkedList<>();
        setSpec2.add("test");
        setSpec2.add("test,\" with separator and quotes");
        OaiHeader header_2 = new OaiHeader(recordIdentifier, dateStamp_2, setSpec2, true);
        expectedHeaders.add(header_2);
        persistenceService.addOrUpdateHeaders(expectedHeaders);

        List<OaiHeader> actualHeaders = persistenceService.getOaiHeaders();
        assertEquals(expectedHeaders, actualHeaders);
    }

    @Test
    public void deleteSelectedHeaders() throws Exception {

        // store 3 headers in db
        List<OaiHeader> headersToStore = new LinkedList<>();
        String recordIdentifier_1 = "oai:example.org:qucosa:123";
        Date dateStamp_1 = DatatypeConverter.parseDateTime("2016-07-20T11:22:57Z").getTime();
        OaiHeader header_1 = new OaiHeader(recordIdentifier_1, dateStamp_1, false);
        headersToStore.add(header_1);
        String recordIdentifier_2 = "oai:example.org:qucosa:456";
        Date dateStamp_2 = DatatypeConverter.parseDateTime("2012-03-30T06:54:12Z").getTime();
        OaiHeader header_2 = new OaiHeader(recordIdentifier_2, dateStamp_2, true);
        headersToStore.add(header_2);
        String recordIdentifier_3 = "oai:example.org:qucosa:789";
        Date dateStamp_3 = DatatypeConverter.parseDateTime("2010-04-14T03:27:52Z").getTime();
        OaiHeader header_3 = new OaiHeader(recordIdentifier_3, dateStamp_3, true);
        headersToStore.add(header_3);
        persistenceService.addOrUpdateHeaders(headersToStore);

        // remove header_1 and header_2
        List<OaiHeader> headersToRemove = new LinkedList<>();
        headersToRemove.add(header_1);
        headersToRemove.add(header_2);
        List<OaiHeader> headersNotRemoved = persistenceService.removeOaiHeadersIfUnmodified(headersToRemove);

        assertEquals("There are OaiHeaders that have not been removed.", new LinkedList<OaiHeader>(),
                headersNotRemoved);

        List<OaiHeader> actualHeadersInDatabase = persistenceService.getOaiHeaders();
        List<OaiHeader> expectedHeadersInDatabase = new LinkedList<>();
        expectedHeadersInDatabase.add(header_3);
        assertEquals("Only the expected OaiHeaders should be in database.", expectedHeadersInDatabase,
                actualHeadersInDatabase);
    }

    /**
     * Assert that {@link OaiHeader}s that have been modified in database are
     * not deleted.<br />
     * Example:
     * <ol>
     * <li>A header X1 is harvested and written to database</li>
     * <li>A consumer reads X1 from database and processes it (e.g. send request
     * to METS dissemination)</li>
     * <li>The harvester writes a new version X2 to database (the
     * recordIdentifiers of X1 and X2 are equal; the document has been modified
     * at the OAI data provider)</li>
     * <li>The consumer finished processing X1 and tries to remove it from
     * database. Since X1 has been modified to X2, X2 must not be removed from
     * database since it is unknown if response received by the consumer
     * contained details to version X1 or X2. An additional request for X2 is
     * necessary.</li>
     * </ol>
     *
     * @throws Exception
     */
    @Test
    public void doNotDeleteModifiedHeader() throws Exception {

        List<OaiHeader> initialHeaders = new LinkedList<>();

        String recordIdentifier = "oai:example.org:qucosa:123";
        Date dateStamp_1 = DatatypeConverter.parseDateTime("2012-03-30T06:54:12Z").getTime();
        OaiHeader header_1 = new OaiHeader(recordIdentifier, dateStamp_1, false);
        initialHeaders.add(header_1);
        persistenceService.addOrUpdateHeaders(initialHeaders);

        List<OaiHeader> updatedHeaders = new LinkedList<>();
        Date dateStamp_2 = DatatypeConverter.parseDateTime("2016-07-20T11:22:57Z").getTime();
        OaiHeader header_2 = new OaiHeader(recordIdentifier, dateStamp_2, false);
        updatedHeaders.add(header_2);
        persistenceService.addOrUpdateHeaders(updatedHeaders);

        List<OaiHeader> headersNotRemoved = persistenceService.removeOaiHeadersIfUnmodified(initialHeaders);
        assertEquals("The OaiHeaders must not have been removed.", initialHeaders, headersNotRemoved);

        List<OaiHeader> actualHeaders = persistenceService.getOaiHeaders();
        assertEquals("The updated OaiHeader should be the only header in database.", updatedHeaders, actualHeaders);
    }

    /* ---- End OaiHeader tests ---- */

    @Before
    public void setUp() throws Exception {

        testPersistenceService.executeQueriesFromFile(TRUNCATE_TABLES_SQL);
        persistenceService = new PostgrePersistenceService(DATABASE_URL, DATABASE_USER, DATABASE_PASSWORD);
    }

    /**
     * Initialize test database: create tables and sequences if they do not
     * exist. <br />
     * In case of an error, make sure the PostgreSQL server is running, the
     * database {@link #DATABASE_URL} has been created externally and
     * {@link DATABASE_USER} with {@link #DATABASE_PASSWORD} does exist with
     * required privileges. Execute /persistence/createRoleAndDatabase.sql by an
     * database admin if this has never been done before.
     *
     * @throws Exception
     */
    @BeforeClass
    public static void initPostgreDB() throws Exception {
        testPersistenceService = new PostgrePersistenceServiceTestHelper(DATABASE_URL, DATABASE_USER,
                DATABASE_PASSWORD);
        testPersistenceService.executeQueriesFromFile(CREATE_SEQUENCES_AND_TABLES_SQL);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        testPersistenceService.executeQueriesFromFile(TRUNCATE_TABLES_SQL);
    }

    private Date now() {
        return Calendar.getInstance().getTime();
    }

}
