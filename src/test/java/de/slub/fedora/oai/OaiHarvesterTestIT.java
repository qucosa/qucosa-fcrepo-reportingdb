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

package de.slub.fedora.oai;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jdt.annotation.NonNull;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import de.slub.persistence.PersistenceService;
import de.slub.persistence.PostgrePersistenceService;
import de.slub.persistence.PostgrePersistenceServiceTestHelper;
import de.slub.persistence.ReportingProperties;
import de.slub.util.TerminateableRunnable;

public class OaiHarvesterTestIT {


	private static final boolean FCREPO3_COMPATIBILITY_MODE = false;

	private static final String OAI_LIST_IDENTIFIERS_XML = "/oai/listIdentifiers.xml";
	private static final String OAI_RESUMPTION_TOKEN_XML = "/oai/resumptionToken.xml";
	private static final String OAI_ERROR_NO_RECORDS_MATCH_XML = "/oai/errorNoRecordsMatch.xml";
	private static final String OAI_ERROR_BAD_RESUMPTION_TOKEN_XML = "/oai/errorBadResumptionToken.xml";
	private static final String OAI_ERROR_MULTIPLE_ERRORS_XML = "/oai/multipleErrors.xml";
	private static final String OAI_EMPTY_RESUMPTION_TOKEN_XML = "/oai/emptyResumptionToken.xml";
	private static final String OAI_IDENTIFIERS_TO_FILTER_XML = "/oai/ListIdentifiersToFilter.xml";

	private static final String CREATE_SEQUENCES_AND_TABLES_SQL = "/persistence/createSequencesAndTables.sql";
	private static final String TRUNCATE_TABLES_SQL = "/persistence/truncateTables.sql";
	private static final String INSERT_OAI_RUN_RESULTS_SQL = "/persistence/insertOAIRunResults.sql";

	// TODO read from Properties file??
	private static final String DATABASE_URL = "jdbc:postgresql://localhost:5432/reportingUnitTest";
	private static final String DATABASE_USER = "reportingDBUnitTest";
	private static final String DATABASE_PASSWORD = "76Sp)qpH2D";


	private EmbeddedHttpHandler embeddedHttpHandler;
	private ReportingProperties reportingProperties = ReportingProperties.getInstance();
	private HttpServer httpServer;
	private PersistenceService persistenceService;
	private static PostgrePersistenceServiceTestHelper testPersistenceService;
	private OaiHarvester oaiHarvester;

	/**
	 * Harvest two OAI headers from ListIdentifiers and put them in
	 * {@link #harvestedHeaders}.
	 * 
	 * @throws Exception
	 */
	@Test
	public void createOaiHeadersForListedIdentifier() throws Exception {
		embeddedHttpHandler.resourcePath = OAI_LIST_IDENTIFIERS_XML;
		runAndWait(oaiHarvester);

		List<OaiHeader> actualOaiHeaders = persistenceService.getOaiHeaders();
		assertEquals("List of harvested headers should contain two elements", 2,
				actualOaiHeaders.size());

		Date datestamp1 = DatatypeConverter.parseDateTime("2014-05-06T17:33:25Z").getTime();

		OaiHeader oaiHeader1 = new OaiHeader("oai:example.org:qucosa:1044", datestamp1, false);

		Date datestamp2 = DatatypeConverter.parseDateTime("2016-07-12T17:33:25Z").getTime();
		// List<String> setSpec = new LinkedList<>();
		// setSpec.add("test:11");
		// setSpec.add("test:22");
		// @SuppressWarnings("null")
		// OaiHeader oaiHeader2 = new OaiHeader("oai:example.org:qucosa:1234",
		// datestamp2, setSpec, true);

		OaiHeader oaiHeader2 = new OaiHeader("oai:example.org:qucosa:1234", datestamp2, true);

		assertTrue(actualOaiHeaders.contains(oaiHeader1));
		assertTrue(actualOaiHeaders.contains(oaiHeader2));
	}

	/**
	 * Test the cleanup of OaiRunResults in database. Assert that the last
	 * inserted statement is always kept, even if it is older than the specified
	 * last result to keep.
	 * 
	 * @throws Exception
	 */
	@Test
	public void cleanupOaiRunResultHistoryAlwaysKeepLastResult() throws Exception {

		// write 3 OaiRunResults to database that are older than one day
		testPersistenceService.executeQueriesFromFile(INSERT_OAI_RUN_RESULTS_SQL);
		assertEquals("Wrong number of OaiRunResults in database, precondition of test failed!", 3,
				testPersistenceService.countOaiRunResults());

		// do cleanup
		SimpleDateFormat dateFormatLastRunToKeep = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		dateFormatLastRunToKeep.setTimeZone(TimeZone.getTimeZone("GMT+2:00"));
		Date lastRunToKeep = dateFormatLastRunToKeep.parse("2016-07-22 13:22:57.137+02");
		persistenceService.cleanupOaiRunResults(lastRunToKeep);

		// check that only one result is remaining in database
		assertEquals("Wrong number of OaiRunResults in database.", 1, testPersistenceService.countOaiRunResults());

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
	 * Test the cleanup of OaiRunResults in database. Assert that all
	 * OaiRunResults are kept that are newer than the specified oldest
	 * OaiRunResult to keep. The test simulates three old results to delete and
	 * two new results to keep.<br />
	 * The test also involves the OaiHarvester, asserting that the cleanup is
	 * triggered after a successful run.
	 * 
	 * @throws Exception
	 */
	@Test
	public void cleanupOaiRunResultHistoryTimestampFilter() throws Exception {

		// write 3 OaiRunResults to database that are older than one day
		testPersistenceService.executeQueriesFromFile(INSERT_OAI_RUN_RESULTS_SQL);

		// write a 4th result to database that must not be deleted
		OaiRunResult fourthResult = new OaiRunResult(now(), now(), "", null, null);
		persistenceService.storeOaiRunResult(fourthResult);

		assertEquals("Wrong number of OaiRunResults in database, precondition of test failed!", 4,
				testPersistenceService.countOaiRunResults());

		// new OaiHarvester that keeps a history of 1 day
		oaiHarvester = new OaiHarvesterBuilder(new URI("http://localhost:8000/fedora/oai"), persistenceService)
				.setPollingInterval(Duration.standardSeconds(1))
				.setOaiRunResultHistory(Duration.standardDays(1))
				.build();

		// just let the harvester do something to have one successful loop,
		// writing a 5th OaiRunResult to database and invoke the cleanup
		// the 5th OaiRunResult's details are read from OAI_RESUMPTION_TOKEN_XML
		embeddedHttpHandler.resourcePath = OAI_RESUMPTION_TOKEN_XML;
		runAndWait(oaiHarvester);

		// check that only two results are remaining in database
		assertEquals("Wrong number of OaiRunResults in database.", 2, testPersistenceService.countOaiRunResults());

		// Make sure the 5th OaiRunResult is returned as the most recently
		// inserted. It's timestampOfRun can not be checked since it was set by
		// the harvester
		// itself.
		Date responseDate = DatatypeConverter.parseDateTime("2014-06-08T11:43:00Z").getTime();
		String token = "111111111111111";
		Date expirationDate = DatatypeConverter.parseDateTime("2014-06-09T18:34:15Z").getTime();
		OaiRunResult lastRun = persistenceService.getLastOaiRunResult();
		assertNotNull(lastRun);
		assertEquals("Response dates are not equal, the OaiRunResult inserted last was deleted.", responseDate,
				lastRun.getResponseDate());
		assertEquals("Resumption tokens are not equal, the OaiRunResult inserted last was deleted.", token,
				lastRun.getResumptionToken());
		assertEquals("Resumption token expiration dates are not equal, the OaiRunResult inserted last was deleted.",
				expirationDate, lastRun.getResumptionTokenExpirationDate());

		// TODO assert, that the other OaiRunResult in database is the one that
		// had been inserted as number four
	}

	/**
	 * Test that there is no cleanup of OaiRunResults if the last run was not
	 * successful.
	 *
	 * @throws Exception
	 */
	@Test
	public void noCleanupOfOaiRunResultHistoryAfterUnsuccessfulRun() throws Exception {

		// write 3 OaiRunResults to database that are older than one day
		testPersistenceService.executeQueriesFromFile(INSERT_OAI_RUN_RESULTS_SQL);
		assertEquals("Wrong number of OaiRunResults in database, precondition of test failed!", 3,
				testPersistenceService.countOaiRunResults());

		// new OaiHarvester that keeps a history of 1 day
		oaiHarvester = new OaiHarvesterBuilder(new URI("http://localhost:8000/fedora/oai"), persistenceService)
				.setPollingInterval(Duration.standardSeconds(1))
				.setOaiRunResultHistory(Duration.standardDays(1))
				.build();

		// let the harvester receive a http 404 to have one UNsuccessful loop,
		// NOT writing a 4th OaiRunResult to database and invoke the cleanup
		embeddedHttpHandler.resourcePath = OAI_RESUMPTION_TOKEN_XML;
		embeddedHttpHandler.httpStatusCode = 404;
		runAndWait(oaiHarvester);

		// check that there are still three results remaining in database
		assertEquals("Wrong number of OaiRunResults in database.", 3, testPersistenceService.countOaiRunResults());

		// make sure the 3rd OaiRunResult is returned as the most recently
		// inserted. look for '2016-07-20 13:22:57.137+02', '2011-01-03
		// 11:00:23-03',
		// '140225245500000', '2014-06-09 20:34:15+04', '2016-07-20
		// 13:18:40.038+02'and be aware of time
		// zones
		OaiRunResult actual = persistenceService.getLastOaiRunResult();
		assertNotNull("Could not load any OaiRunResult from database", actual);

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

	/*----  test filtering of harvested OAI headers  ----*/

	/**
	 * Process a OAI response with 13 header elements, use filter to keep only 6
	 * header elements that contain "real" Qucosa documents.
	 * 
	 * @throws Exception
	 */
	@Test
	public void filterHarvestedOaiHeaders() throws Exception {

		oaiHarvester = new OaiHarvesterBuilder(new URI("http://localhost:8000/fedora/oai"), persistenceService)
				.setPollingInterval(Duration.standardSeconds(1))
				.setOaiHeaderFilter(new QucosaDocumentFilter())
				.build();


		embeddedHttpHandler.resourcePath = OAI_IDENTIFIERS_TO_FILTER_XML;
		runAndWait(oaiHarvester);
		assertEquals("Wrong number of headers, filter does not work.", 6, persistenceService.getOaiHeaders().size());

		List<String> actualIDs = new LinkedList<>();
		for (OaiHeader header : persistenceService.getOaiHeaders()) {
			actualIDs.add(header.getRecordIdentifier());
		}

		String[] expectedIDs = { "oai:example.org:qucosa:1", "oai:example.org:qucosa:22", "oai:example.org:qucosa:333",
				"oai:example.org:qucosa:4444", "oai:example.org:qucosa:55555", "oai:example.org:qucosa:666666" };

		for (String expectedID : expectedIDs) {
			assertTrue("Missing expected recordIdentifier " + expectedID, actualIDs.contains(expectedID));
		}
	}

	/*---- Begin test logic for processing of resumption tokens and nextFromValues  ----*/

	/*---- Begin test 4 combinations of last OaiRunResult's resumptionToken and nextFromTimestamp to build GET request ---- */

	/**
	 * Simulate initial run. No {@link OaiRunResult} is present, so no
	 * resumptionToken and no nextFromtimestamp are present. Harvester must
	 * request all data.
	 * 
	 * @throws Exception
	 */
	@Test
	public void createGETfromLastOaiRunResultNoResumptionTokenNoNextFromTimestamp() throws Exception {

		// just let the harvester do something to have one successful loop
		// (OAI data provider's response is ignored)
		embeddedHttpHandler.resourcePath = OAI_RESUMPTION_TOKEN_XML;
		runAndWait(oaiHarvester);

		assertFalse("No from parameter in OAI query allowed",
				embeddedHttpHandler.lastRequestUri.getQuery().contains("from="));
		assertFalse("No resumptionToken parameter in OAI query allowed",
				embeddedHttpHandler.lastRequestUri.getQuery().contains("resumptionToken="));
		assertTrue("OAI query must contain parameter metadataPrefix with value oai_dc",
				embeddedHttpHandler.lastRequestUri.getQuery().contains("metadataPrefix=oai_dc"));
	}

	/**
	 * Last {@link OaiRunResult} contains resumptionToken and no
	 * nextFromtimestamp. Harvester must use resumptionToken in GET request.
	 * 
	 * @throws Exception
	 */
	@Test
	public void createGETfromLastOaiRunResultWithResumptionTokenNoNextFromTimestamp() throws Exception {

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		Date timestampLastRun = dateFormat.parse("2015-01-01T01:01:00");
		Date responseDate = dateFormat.parse("2015-01-01T01:01:05");
		String resumptionToken = "111222333";
		Date resumptionTokenExpirationDate = dateFormat.parse("2015-01-01T01:09:25");
		Date nextFromTimestamp = null;

		OaiRunResult lastRun = new OaiRunResult(timestampLastRun, responseDate, resumptionToken,
				resumptionTokenExpirationDate, nextFromTimestamp);
		persistenceService.storeOaiRunResult(lastRun);

		// just let the harvester do something to have one successful loop
		// (OAI data provider's response is ignored)
		embeddedHttpHandler.resourcePath = OAI_RESUMPTION_TOKEN_XML;
		runAndWait(oaiHarvester);

		assertFalse("No from parameter in OAI query allowed",
				embeddedHttpHandler.lastRequestUri.getQuery().contains("from="));
		assertTrue("Parameter resumptionToken with value '111222333' in OAI query required",
				embeddedHttpHandler.lastRequestUri.getQuery().contains("resumptionToken=111222333"));
		assertFalse("OAI query must not contain parameter metadataPrefix if resumptionToken is present",
				embeddedHttpHandler.lastRequestUri.getQuery().contains("metadataPrefix"));
	}

	/**
	 * Last {@link OaiRunResult} contains a resumptionToken and also a
	 * nextFromtimestamp. Harvester must use resumptionToken in GET request,
	 * nextFromtimestamp must be ignored in request. (nextFromtimestamp is
	 * backed up for next {@link OaiRunResult} but this is tested separately.)
	 * 
	 * @throws Exception
	 */
	@Test
	public void createGETfromLastOaiRunResultWithResumptionTokenAndNextFromTimestamp() throws Exception {

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		Date timestampLastRun = dateFormat.parse("2015-01-01T01:01:00");
		Date responseDate = dateFormat.parse("2015-01-01T01:01:05");
		String resumptionToken = "111222333";
		Date resumptionTokenExpirationDate = dateFormat.parse("2015-01-01T01:09:25");
		Date nextFromTimestamp = dateFormat.parse("2015-02-02T02:02:02");

		OaiRunResult lastRun = new OaiRunResult(timestampLastRun, responseDate, resumptionToken,
				resumptionTokenExpirationDate, nextFromTimestamp);
		persistenceService.storeOaiRunResult(lastRun);

		// just let the harvester do something to have one successful loop
		// (OAI data provider's response is ignored)
		embeddedHttpHandler.resourcePath = OAI_RESUMPTION_TOKEN_XML;
		runAndWait(oaiHarvester);

		assertFalse("No from parameter in OAI query allowed",
				embeddedHttpHandler.lastRequestUri.getQuery().contains("from="));
		assertTrue("Parameter resumptionToken with value '111222333' in OAI query required",
				embeddedHttpHandler.lastRequestUri.getQuery().contains("resumptionToken=111222333"));
		assertFalse("OAI query must not contain parameter metadataPrefix if resumptionToken is present",
				embeddedHttpHandler.lastRequestUri.getQuery().contains("metadataPrefix"));
	}

	/**
	 * Last {@link OaiRunResult} contains no resumptionToken but a
	 * nextFromtimestamp. Harvester must use nextFromtimestamp in GET request.
	 * 
	 * @throws Exception
	 */
	@Test
	public void createGETfromLastOaiRunResultNoResumptionTokenWithNextFromTimestamp() throws Exception {

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		Date timestampLastRun = dateFormat.parse("2015-01-01T01:01:00");
		Date responseDate = dateFormat.parse("2015-01-01T01:01:05");
		String resumptionToken = "";
		Date resumptionTokenExpirationDate = null;
		Date nextFromTimestamp = dateFormat.parse("2015-02-02T02:02:02");

		OaiRunResult lastRun = new OaiRunResult(timestampLastRun, responseDate, resumptionToken,
				resumptionTokenExpirationDate, nextFromTimestamp);
		persistenceService.storeOaiRunResult(lastRun);

		// just let the harvester do something to have one successful loop
		// (OAI data provider's response is ignored)
		embeddedHttpHandler.resourcePath = OAI_RESUMPTION_TOKEN_XML;
		runAndWait(oaiHarvester);

		assertTrue("Parameter from with value 2015-02-02T02:02:02 in OAI query required.",
				embeddedHttpHandler.lastRequestUri.getQuery().contains("from=2015-02-02T02:02:02"));
		assertFalse("No resumptionToken parameter in OAI query allowed",
				embeddedHttpHandler.lastRequestUri.getQuery().contains("resumptionToken="));
		assertTrue("OAI query must contain parameter metadataPrefix with value oai_dc",
				embeddedHttpHandler.lastRequestUri.getQuery().contains("metadataPrefix=oai_dc"));
	}

	/*---- End test 4 combinations of last OaiRunResult's resumptionToken and nextFromTimestamp to build GET request ---- */

	/*---- Begin create new OaiRunResult based on last OaiRunResult and OAI data provider's response ----*/

	/**
	 * Test the very first run of the harvester: no (last) {@link OaiRunResult}
	 * is present, the OAI data provider returns a resumption token.<br />
	 * Parse OAI responseDate, resumptionToken, expiration date and from OAI
	 * data provider's response and store it as new {@link OaiRunResult} in
	 * database. Assert, that this {@link OaiRunResult}'s nextFromTimestamp is
	 * {@code null}
	 * 
	 * @throws Exception
	 */
	@Test
	public void createOaiRunResultFromResumptionToken() throws Exception {

		assertNull("Precondition faild, there must not be any last OaiRunResult",
				persistenceService.getLastOaiRunResult());

		embeddedHttpHandler.resourcePath = OAI_RESUMPTION_TOKEN_XML;
		runAndWait(oaiHarvester);

		OaiRunResult lastResult = persistenceService.getLastOaiRunResult();
		assertNotNull("No OaiRunResult was returned by persistence layer", lastResult);

		Date expectedResponseDate = parseDateTime("2014-06-08T11:43:00Z");
		String expectedResumptionToken = "111111111111111";
		Date expectedExpirationDate = parseDateTime("2014-06-09T18:34:15Z");

		// compare all of OaiRunResults values except timestampOfRun which is
		// generated by the harvester at runtime
		assertEquals(expectedResponseDate, lastResult.getResponseDate());
		assertEquals(expectedResumptionToken, lastResult.getResumptionToken());
		assertEquals(expectedExpirationDate, lastResult.getResumptionTokenExpirationDate());
		assertEquals("There must not be any nextFromTimestamp. On error, we need to request all data, "
				+ "not excluding anything by from-parameter.", null, lastResult.getNextFromTimestamp());
	}

	/**
	 * Similar to {@link #createOaiRunResultFromResumptionToken()}, but here, a
	 * last {@link OaiRunResult} with a nextFromTimestamp is present. The OAI
	 * data provider returns a resumption token.<br />
	 * Parse OAI responseDate, resumptionToken, expiration date and from OAI
	 * data provider's response and store it as new {@link OaiRunResult} in
	 * database. Assert, that this {@link OaiRunResult}'s nextFromTimestamp is
	 * the same as the last {@link OaiRunResult}'s nextFromTimestamp.
	 * 
	 * @throws Exception
	 */
	@Test
	public void createOaiRunResultFromResumptionTokenCopyNextFromTimestamp() throws Exception {

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		Date timestampLastRun = dateFormat.parse("2014-01-01T01:01:00");
		Date responseDate = dateFormat.parse("2014-01-01T01:01:05");
		String resumptionToken = "";
		Date resumptionTokenExpirationDate = null;
		Date nextFromTimestamp = dateFormat.parse("2014-02-02T02:02:02");

		OaiRunResult lastRun = new OaiRunResult(timestampLastRun, responseDate, resumptionToken,
				resumptionTokenExpirationDate, nextFromTimestamp);
		persistenceService.storeOaiRunResult(lastRun);

		embeddedHttpHandler.resourcePath = OAI_RESUMPTION_TOKEN_XML;
		runAndWait(oaiHarvester);

		OaiRunResult lastResult = persistenceService.getLastOaiRunResult();
		assertNotNull("No OaiRunResult was returned by persistence layer", lastResult);

		Date expectedResponseDate = parseDateTime("2014-06-08T11:43:00Z");
		String expectedResumptionToken = "111111111111111";
		Date expectedExpirationDate = parseDateTime("2014-06-09T18:34:15Z");

		// compare all of OaiRunResults values except timestampOfRun which is
		// generated by the harvester at runtime
		assertEquals(expectedResponseDate, lastResult.getResponseDate());
		assertEquals(expectedResumptionToken, lastResult.getResumptionToken());
		assertEquals(expectedExpirationDate, lastResult.getResumptionTokenExpirationDate());
		assertEquals("The nextFromTimestamp should have been copied from the last OaiRunResult.", nextFromTimestamp,
				lastResult.getNextFromTimestamp());
	}

	/**
	 * The OAI data provides's response contains an empty resumptionToken, the
	 * new {@link OaiRunResult} to store must have a nextFromTimestamp equal to
	 * its timestampOfRun.
	 * 
	 * @throws Exception
	 */
	@Test
	public void createOaiRunResultExpectedEmptyResumptionTokenNewNextFromTimestamp() throws Exception {

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		Date timestampLastRun = dateFormat.parse("2014-01-01T01:01:00");
		Date responseDate = dateFormat.parse("2014-01-01T01:01:05");
		String resumptionToken = "111222333";
		Date resumptionTokenExpirationDate = null;
		Date nextFromTimestamp = dateFormat.parse("2014-02-02T02:02:02");

		OaiRunResult lastRun = new OaiRunResult(timestampLastRun, responseDate, resumptionToken,
				resumptionTokenExpirationDate, nextFromTimestamp);
		persistenceService.storeOaiRunResult(lastRun);

		// use any "normal" response without resumptionToken
		embeddedHttpHandler.resourcePath = OAI_EMPTY_RESUMPTION_TOKEN_XML;
		runAndWait(oaiHarvester);

		OaiRunResult actualOaiRunResult = persistenceService.getLastOaiRunResult();
		assertNotNull("No OaiRunResult was returned by persistence layer", actualOaiRunResult);

		assertEquals(actualOaiRunResult.getTimestampOfRun(), actualOaiRunResult.getNextFromTimestamp());
	}

	/**
	 * The OAI data provides's response does not contain a resumptionToken, the
	 * new {@link OaiRunResult} to store must have a nextFromTimestamp equal to
	 * its timestampOfRun. This is independent from any last
	 * {@link OaiRunResult}.
	 * 
	 * @throws Exception
	 */
	@Test
	public void createOaiRunResultNoResumptionTokenNewNextFromTimestamp() throws Exception {

		// use any "normal" response without resumptionToken
		embeddedHttpHandler.resourcePath = OAI_LIST_IDENTIFIERS_XML;
		runAndWait(oaiHarvester);

		OaiRunResult actualOaiRunResult = persistenceService.getLastOaiRunResult();
		assertNotNull("No OaiRunResult was returned by persistence layer", actualOaiRunResult);

		assertEquals(actualOaiRunResult.getTimestampOfRun(), actualOaiRunResult.getNextFromTimestamp());
	}

	/* --- test two exceptions against specification --- */

	/**
	 * The OAI data provides's response that contains the last part of a
	 * paginated response must contain an empty resumption token. If this token
	 * is missing, the whole paginated list will be requested again, i.e., the
	 * new {@link OaiRunResult} to store must have a nextFromTimestamp equal the
	 * last {@link OaiRunResult}'s nextFromTimestamp.<br />
	 * anyhow, it seems that Fedora Commons 3 has a bug not closing an paginated
	 * result with an empty resumption token... if tets are tun in
	 * {@link #FCREPO3_COMPATIBILITY_MODE},
	 * 
	 * @throws Exception
	 */
	@Test
	public void createOaiRunResultFromMissingEmptyResumptionToken() throws Exception {

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		Date timestampLastRun = dateFormat.parse("2014-01-01T01:01:00");
		Date responseDate = dateFormat.parse("2014-01-01T01:01:05");
		String resumptionToken = "111222333";
		Date resumptionTokenExpirationDate = null;
		Date nextFromTimestamp = dateFormat.parse("2014-02-02T02:02:02");

		OaiRunResult lastRun = new OaiRunResult(timestampLastRun, responseDate, resumptionToken,
				resumptionTokenExpirationDate, nextFromTimestamp);
		persistenceService.storeOaiRunResult(lastRun);

		// use any "normal" response without resumptionToken
		embeddedHttpHandler.resourcePath = OAI_LIST_IDENTIFIERS_XML;
		runAndWait(oaiHarvester);

		OaiRunResult actualOaiRunResult = persistenceService.getLastOaiRunResult();
		assertNotNull("No OaiRunResult was returned by persistence layer", actualOaiRunResult);

		if (FCREPO3_COMPATIBILITY_MODE) {
			assertEquals("nextFromTimestamp must have the value of the current timestampOfRun.",
					actualOaiRunResult.getTimestampOfRun(), actualOaiRunResult.getNextFromTimestamp());
		} else {

			assertEquals("nextFromTimestamp must have the value from the previous run's nextFromTimestamp.",
					actualOaiRunResult.getTimestampOfRun(), actualOaiRunResult.getNextFromTimestamp());
		}
	}

	/**
	 * An empty resumptionToken is allowed only if the previous response
	 * contained a non-empty resumption token. In case of this error, the last
	 * GET request is repeated.
	 */
	@Test
	public void createOaiRunResultFromUnexpectedEmptyResumptionToken() throws Exception {

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		Date timestampLastRun = dateFormat.parse("2014-01-01T01:01:00");
		Date responseDate = dateFormat.parse("2014-01-01T01:01:05");
		String resumptionToken = "";
		Date resumptionTokenExpirationDate = null;
		Date nextFromTimestamp = dateFormat.parse("2014-02-02T02:02:02");

		OaiRunResult lastRun = new OaiRunResult(timestampLastRun, responseDate, resumptionToken,
				resumptionTokenExpirationDate, nextFromTimestamp);
		persistenceService.storeOaiRunResult(lastRun);

		embeddedHttpHandler.resourcePath = OAI_EMPTY_RESUMPTION_TOKEN_XML;
		runAndWait(oaiHarvester);

		OaiRunResult actualOaiRunResult = persistenceService.getLastOaiRunResult();
		assertNotNull("No OaiRunResult was returned by persistence layer", actualOaiRunResult);

		assertEquals("nextFromTimestamp must have the value from the previous run.", nextFromTimestamp,
				actualOaiRunResult.getNextFromTimestamp());
	}

	/*---- End create new OaiRunResult based on last OaiRunResult and OAI data provider's response ----*/

	/*---- Begin create new OaiRunResult based on last OaiRunResult and OAI-PMH errors in data provider's response ----*/

	/**
	 * If the harvester receives a OAI-PMH error 'noRecordsMatch', the next
	 * request should contain the GET parameter from with the timestamp of the
	 * current run. Therefore, this run's {@link OaiRunResult}'s resumption
	 * token must be empty and the nextFromTimestamp has the same value as this
	 * run's timestampOfRun.
	 * 
	 * @throws Exception
	 */
	@Test
	public void errorNoRecordsMatchInResponse() throws Exception {

		embeddedHttpHandler.resourcePath = OAI_ERROR_NO_RECORDS_MATCH_XML;
		runAndWait(oaiHarvester);

		OaiRunResult actualOaiRunResult = persistenceService.getLastOaiRunResult();
		assertNotNull(actualOaiRunResult);
		assertNull("Resumption token must not exist.", actualOaiRunResult.getResumptionToken());
		assertEquals(actualOaiRunResult.getTimestampOfRun(), actualOaiRunResult.getNextFromTimestamp());
	}

	/**
	 * If an OAI data providers response contains the error badResumptionToken,
	 * the harvester must use {@link OaiRunResult#getNextFromTimestamp()} of the
	 * last successful run in the next request.
	 * 
	 * @throws Exception
	 */
	@Test
	public void errorBadResumptionTokenInResponse() throws Exception {

		Date initialLastRun = parseDateTime("2014-06-08T11:43:00Z");
		Date initialResponseDate = parseDateTime("2014-06-08T11:43:00Z");
		String initialResumptionToken = "111111111111111";
		Date initialNextRunTimestamp = parseDateTime("2014-06-04T11:11:11Z");

		OaiRunResult initialOaiRunResult = new OaiRunResult(initialLastRun, initialResponseDate, initialResumptionToken,
				null, initialNextRunTimestamp);
		persistenceService.storeOaiRunResult(initialOaiRunResult);

		embeddedHttpHandler.resourcePath = OAI_ERROR_BAD_RESUMPTION_TOKEN_XML;
		runAndWait(oaiHarvester);

		OaiRunResult actualOaiRunResult = persistenceService.getLastOaiRunResult();
		assertNotNull(actualOaiRunResult);

		// compare all of OaiRunResults values except timestampOfRun which is
		// generated by the harvester at runtime
		Date expectedResponseDate = parseDateTime("2016-07-26T18:05:24Z");
		String expectedResumptionToken = null;
		Date expectedNextRunTimestamp = initialNextRunTimestamp;

		assertEquals(expectedResponseDate, actualOaiRunResult.getResponseDate());
		assertEquals(expectedResumptionToken, actualOaiRunResult.getResumptionToken());
		assertEquals(expectedNextRunTimestamp, actualOaiRunResult.getNextFromTimestamp());
	}

	/**
	 * If an OAI data providers response contains errors other than
	 * badResumptionToken and noRecordsMatch, this run's {@link OaiRunResult}'s
	 * resumption token must be empty and the nextFromTimestamp is backed up
	 * from the previous run.
	 * 
	 * @throws Exception
	 */
	@Test
	public void errorMultipleErrorsInResponse() throws Exception {

		Date initialLastRun = parseDateTime("2014-06-08T11:43:00Z");
		Date initialResponseDate = parseDateTime("2014-06-08T11:43:00Z");
		String initialResumptionToken = "111111111111111";
		Date initialNextRunTimestamp = parseDateTime("2014-06-04T11:11:11Z");

		OaiRunResult initialOaiRunResult = new OaiRunResult(initialLastRun, initialResponseDate, initialResumptionToken,
				null, initialNextRunTimestamp);
		persistenceService.storeOaiRunResult(initialOaiRunResult);

		embeddedHttpHandler.resourcePath = OAI_ERROR_MULTIPLE_ERRORS_XML;
		runAndWait(oaiHarvester);

		OaiRunResult actualOaiRunResult = persistenceService.getLastOaiRunResult();
		assertNotNull(actualOaiRunResult);

		// do not compare timestampOfRun which is generated by the harvester at
		// runtime
		Date expectedResponseDate = parseDateTime("2016-07-26T18:05:24Z");

		Date expectedNextRunTimestamp = initialNextRunTimestamp;

		assertEquals(expectedResponseDate, actualOaiRunResult.getResponseDate());
		assertNull("Resumption token must not exist.", actualOaiRunResult.getResumptionToken());
		assertEquals(expectedNextRunTimestamp, actualOaiRunResult.getNextFromTimestamp());
	}

	/*---- End create new OaiRunResult based on last OaiRunResult and OAI-PMH errors in data provider's response ----*/
	/*---- End test logic for processing of resumption tokens and nextFromValues  ----*/

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

	@Before
	public void setUp() throws Exception {

		testPersistenceService.executeQueriesFromFile(TRUNCATE_TABLES_SQL);
		persistenceService = new PostgrePersistenceService(DATABASE_URL, DATABASE_USER, DATABASE_PASSWORD);
		createOaiHarvester();
		setupHttpServer();
	}

	@After
	public void stopHttpServer() {
		httpServer.stop(1);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		testPersistenceService.executeQueriesFromFile(TRUNCATE_TABLES_SQL);
	}

	private void createOaiHarvester() throws Exception {
		oaiHarvester = new OaiHarvesterBuilder(new URI("http://localhost:8000/fedora/oai"), persistenceService)
				.setPollingInterval(Duration.standardSeconds(1))
				.build();
	}

	private void runAndWait(TerminateableRunnable runnable) throws InterruptedException {
		Thread thread = new Thread(runnable);
		thread.start();
		TimeUnit.MILLISECONDS.sleep(1000);
		runnable.terminate();
		thread.join();
	}

	private void setupHttpServer() throws IOException {
		httpServer = HttpServer.create(new InetSocketAddress(8000), 0);
		embeddedHttpHandler = new EmbeddedHttpHandler();
		httpServer.createContext("/fedora/oai", embeddedHttpHandler);
		httpServer.setExecutor(null); // creates a default executor
		httpServer.start();
	}

	private Date now() {
		return Calendar.getInstance().getTime();
	}

	@NonNull
	private Date parseDateTime(@NonNull String timestamp) throws IllegalArgumentException {
		if (StringUtils.isBlank(timestamp)) {
			throw new IllegalArgumentException("timestamp must not be null or empty");
		}
		return DatatypeConverter.parseDateTime(timestamp).getTime();
	}

	class EmbeddedHttpHandler implements HttpHandler {

		public URI lastRequestUri;
		public String resourcePath;
		public int httpStatusCode = 200;

		@Override
		public void handle(HttpExchange exchange) throws IOException {
			lastRequestUri = exchange.getRequestURI();
			exchange.sendResponseHeaders(httpStatusCode, 0);
			IOUtils.copy(this.getClass().getResourceAsStream(resourcePath), exchange.getResponseBody());
			exchange.getResponseBody().close();
		}
	}

}
