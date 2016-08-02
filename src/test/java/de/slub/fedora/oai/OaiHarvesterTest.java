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

import static org.mockito.Mockito.never;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import de.slub.persistence.PersistenceService;
import de.slub.persistence.PostgrePersistenceServiceTestHelper;
import de.slub.persistence.ReportingProperties;
import de.slub.util.TerminateableRunnable;

public class OaiHarvesterTest {

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
	private PersistenceService mockedPersistenceService;
	private OaiHarvester oaiHarvester;

	@Captor
	private ArgumentCaptor<List<OaiHeader>> oaiHeaderCaptor;
	
	
	/**
	 * Harvest two OAI headers from ListIdentifiers and store them in
	 * persistence layer.
	 * 
	 * @throws Exception
	 */
	@Test
	public void createOaiHeadersForListedIdentifier() throws Exception {
		embeddedHttpHandler.resourcePath = OAI_LIST_IDENTIFIERS_XML;
		runAndWait(oaiHarvester);

		List<OaiHeader> expectedHeaders = new LinkedList<>();
		Date datestamp1 = DatatypeConverter.parseDateTime("2014-05-06T17:33:25Z").getTime();
		OaiHeader oaiHeader1 = new OaiHeader("oai:example.org:qucosa:1044", datestamp1, false);
		expectedHeaders.add(oaiHeader1);

		Date datestamp2 = DatatypeConverter.parseDateTime("2016-07-12T17:33:25Z").getTime();
		List<String> setSpec = new LinkedList<>();
		setSpec.add("test:11");
		setSpec.add("test:22");
		OaiHeader oaiHeader2 = new OaiHeader("oai:example.org:qucosa:1234", datestamp2, setSpec, true);
		expectedHeaders.add(oaiHeader2);
		
		verify(mockedPersistenceService).addOrUpdateHeaders(expectedHeaders);
	}

	
	/**
	 * If the run was not successful, 1) no {@link OaiRunResult} is persisted
	 * and 2) no cleanup of previous {@link OaiRunResult}s is done.
	 *
	 * @throws Exception
	 */
	@Test
	public void noCleanupOfOaiRunResultHistoryAfterUnsuccessfulRun() throws Exception {

		// new OaiHarvester that keeps a history of 1 day
		oaiHarvester = new OaiHarvesterBuilder(new URI("http://localhost:8000/fedora/oai"), mockedPersistenceService)
				.setPollingInterval(Duration.standardSeconds(1)).setOaiRunResultHistory(Duration.standardDays(1))
				.build();

		// let the harvester receive a http 404 to have one UNsuccessful loop,
		// NOT writing a 4th OaiRunResult to database and invoke the cleanup
		embeddedHttpHandler.resourcePath = OAI_RESUMPTION_TOKEN_XML;
		embeddedHttpHandler.httpStatusCode = 404;
		runAndWait(oaiHarvester);

		verify(mockedPersistenceService, never()).cleanupOaiRunResults(Mockito.any(Date.class));
		verify(mockedPersistenceService, never()).storeOaiRunResult(Mockito.any(OaiRunResult.class));
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

		oaiHarvester = new OaiHarvesterBuilder(new URI("http://localhost:8000/fedora/oai"), mockedPersistenceService)
				.setPollingInterval(Duration.standardSeconds(1)).setOaiHeaderFilter(new QucosaDocumentFilter()).build();

		embeddedHttpHandler.resourcePath = OAI_IDENTIFIERS_TO_FILTER_XML;
		runAndWait(oaiHarvester);
		
		verify(mockedPersistenceService).addOrUpdateHeaders(oaiHeaderCaptor.capture());
		List<OaiHeader> actualOaiHeaders = (List<OaiHeader>)oaiHeaderCaptor.getValue();
		assertEquals("Wrong number of headers, filter does not work.", 6, actualOaiHeaders.size());

		List<String> actualIDs = new LinkedList<>();
		for (OaiHeader header : actualOaiHeaders) {
			actualIDs.add(header.getRecordIdentifier());
		}

		String[] expectedIDs = { "oai:example.org:qucosa:1", "oai:example.org:qucosa:22", "oai:example.org:qucosa:333",
				"oai:example.org:qucosa:4444", "oai:example.org:qucosa:55555", "oai:example.org:qucosa:666666" };

		for (String expectedID : expectedIDs) {
			assertTrue("Missing expected recordIdentifier " + expectedID, actualIDs.contains(expectedID));
		}
	}
	

	@Before
	public void setUp() throws Exception {

		MockitoAnnotations.initMocks(this);
		mockedPersistenceService = mock(PersistenceService.class);
//		persistenceService = new PostgrePersistenceService(DATABASE_URL, DATABASE_USER, DATABASE_PASSWORD);
		createOaiHarvester();
		setupHttpServer();
	}

	@After
	public void stopHttpServer() {
		httpServer.stop(1);
	}

	@AfterClass
	public static void tearDown() throws Exception {
//		testPersistenceService.executeQueriesFromFile(TRUNCATE_TABLES_SQL);
	}

	private void createOaiHarvester() throws Exception {
		oaiHarvester = new OaiHarvesterBuilder(new URI("http://localhost:8000/fedora/oai"), mockedPersistenceService)
				.setPollingInterval(Duration.standardSeconds(1)).build();
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
