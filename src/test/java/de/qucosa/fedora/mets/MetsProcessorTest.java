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

package de.qucosa.fedora.mets;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import de.qucosa.fedora.oai.OaiHeader;
import de.qucosa.persistence.PersistenceService;
import de.qucosa.util.TerminateableRunnable;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.InputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MetsProcessorTest {

    private static final int SLEEP_TIME_RUN_AND_WAIT = 0;
    private static final Duration POLLING_INTERVAL = Duration.millis(0);
    private static final Duration MINIMUM_WAITTIME_BETWEEN_TWO_REQUESTS = Duration.millis(0);

    private static final String METS_QUCOSA_13_XML = "/mets/qucosa13-mets.xml";
    private static final String METS_QUCOSA_22_XML = "/mets/qucosa22-mets.xml";
    private static final String METS_QUCOSA_7455_XML = "/mets/qucosa7455-mets.xml";
    private static final String METS_QUCOSA_31789_XML = "/mets/qucosa31789-mets.xml";
    private static final String METS_QUCOSA_31790_XML = "/mets/qucosa31790-mets.xml";

    private PersistenceService mockedPersistenceService;
    private CloseableHttpClient mockedHttpClient;
    private CloseableHttpResponse mockedHttpResponse;
    private StatusLine mockedStatusLine;
    private HttpEntity mockedHttpEntity;
    private MetsProcessor metsHarvester;

    @Captor
    private ArgumentCaptor<List<ReportingDocumentMetadata>> reportingDocumentMetadataCaptor;

    @Captor
    private ArgumentCaptor<List<OaiHeader>> oaiHeaderCaptor;



    /**
     * Test standard functionality of {@link MetsProcessor}.<br />
     * Load one {@link OaiHeader} from persistence, query mets dissemination service, parse METS XML and extract data
     * relevant to reporting, store {@link ReportingDocumentMetadata} in persistence and remove {@link OaiHeader} from
     * persistence.
     * 
     * @throws Exception
     */
    @Test
    public void harvestSingleMetsXML() throws Exception {

        // mock persistence returning one OaiHeader
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        Date datestamp = dateFormat.parse("2015-12-17T16:03:17Z");
        String recordIdentifier = "oai:example.org:qucosa:7455";
        OaiHeader qucosa13Header = new OaiHeader(recordIdentifier, datestamp, false);
        List<OaiHeader> oaiHeaders = new LinkedList<>();
        oaiHeaders.add(qucosa13Header);
        when(mockedPersistenceService.getOaiHeaders()).thenReturn(oaiHeaders);

        // mock mets dissemination service
        when(mockedHttpEntity.getContent()).thenAnswer(new Answer<InputStream>() {
            public InputStream answer(InvocationOnMock invocation) {
                return this.getClass().getResourceAsStream(METS_QUCOSA_7455_XML);
            }
        });

        runAndWait(metsHarvester);

        Date distributionDate = new Date(new DateTime("2016-05-24T12:33:56+0200").getMillis());
        ReportingDocumentMetadata expectedReportingDoc = new ReportingDocumentMetadata(recordIdentifier,
                "slub", "in_book", distributionDate, datestamp);

        // assert that ReportingDocumentMetadata has been parsed from mets
        // dissemination and put to persistence
        verify(mockedPersistenceService, atLeastOnce())
                .addOrUpdateReportingDocuments(reportingDocumentMetadataCaptor.capture());
        List<ReportingDocumentMetadata> actualReportingDoc = reportingDocumentMetadataCaptor.getAllValues().get(0);

        assertEquals("Exactly one ReportingDocumentMetadata object should have been persisted", 1,
                actualReportingDoc.size());
        assertEquals("The persisted ReportingDocumentMetadata object is not equal to the expected object.",
                expectedReportingDoc, actualReportingDoc.get(0));

        // assert OaiHeader has been removed from persistence
        verify(mockedPersistenceService, atLeastOnce()).removeOaiHeadersIfUnmodified(oaiHeaderCaptor.capture());
        List<OaiHeader> actualOaiHeaders = oaiHeaderCaptor.getAllValues().get(0);
        assertEquals("Exactly one OaiHeader should have been removed from persistence.", 1, actualOaiHeaders.size());
        assertEquals("The removed OaiHeader object is not equal to the expected object.", oaiHeaders.get(0),
                actualOaiHeaders.get(0));
    }
    

    /**
     * Test standard functionality of {@link MetsProcessor}.<br />
     * Load two {@link OaiHeader}s from persistence, query mets dissemination service, parse METS XML and extract data
     * relevant to reporting, store {@link ReportingDocumentMetadata} objects in persistence and remove both
     * {@link OaiHeader}s from persistence.
     * 
     * @throws Exception
     */
    @Test
    public void harvestMultipleMetsXML() throws Exception {

        // mock persistence returning two OaiHeaders
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        Date datestamp13 = dateFormat.parse("2015-12-17T16:03:17Z");
        String recordIdentifier13 = "oai:example.org:qucosa:13";
        OaiHeader qucosa13Header = new OaiHeader(recordIdentifier13, datestamp13, false);

        Date datestamp22 = dateFormat.parse("2015-12-17T16:03:21Z");
        String recordIdentifier22 = "oai:example.org:qucosa:22";
        OaiHeader qucosa22Header = new OaiHeader(recordIdentifier22, datestamp22, false);

        List<OaiHeader> oaiHeaders = new LinkedList<>();
        oaiHeaders.add(qucosa13Header);
        oaiHeaders.add(qucosa22Header);
        when(mockedPersistenceService.getOaiHeaders()).thenReturn(oaiHeaders);

        // mock mets dissemination service, first response is document
        // qucosa:13, second (and any subsequent) is qucosa:22
        when(mockedHttpEntity.getContent()).thenAnswer(new Answer<InputStream>() {
            public InputStream answer(InvocationOnMock invocation) {
                return this.getClass().getResourceAsStream(METS_QUCOSA_13_XML);
            }
        }).thenAnswer(new Answer<InputStream>() {
            public InputStream answer(InvocationOnMock invocation) {
                return this.getClass().getResourceAsStream(METS_QUCOSA_22_XML);
            }
        });

        runAndWait(metsHarvester);

        // assert that ReportingDocumentMetadata has been parsed from mets
        // dissemination and put to persistence
        Date distributionDate13 = DatatypeConverter.parseDateTime("2008-08-04").getTime();
        ReportingDocumentMetadata expectedReportingDoc13 = new ReportingDocumentMetadata(recordIdentifier13,
                "SLUB", "issue", distributionDate13, datestamp13);

        Date distributionDate22 = DatatypeConverter.parseDateTime("2011-03-31").getTime();
        ReportingDocumentMetadata expectedReportingDoc22 = new ReportingDocumentMetadata(recordIdentifier22,
                "TU Dresden", "issue", distributionDate22, datestamp22);

        verify(mockedPersistenceService, atLeastOnce())
                .addOrUpdateReportingDocuments(reportingDocumentMetadataCaptor.capture());
        List<ReportingDocumentMetadata> actualReportingDoc = reportingDocumentMetadataCaptor.getAllValues().get(0);

        assertEquals("Exactly two ReportingDocumentMetadata objects should have been persisted", 2,
                actualReportingDoc.size());
        assertTrue(expectedReportingDoc13 + " has not been put to persistence.",
                actualReportingDoc.contains(expectedReportingDoc13));
        assertTrue(expectedReportingDoc22 + " has not been put to persistence.",
                actualReportingDoc.contains(expectedReportingDoc22));

        assertEquals("The persisted ReportingDocumentMetadata object is not equal to the expected object.",
                expectedReportingDoc13, actualReportingDoc.get(0));

        // assert both OaiHeaders have been removed from persistence
        verify(mockedPersistenceService, atLeastOnce()).removeOaiHeadersIfUnmodified(oaiHeaderCaptor.capture());
        List<OaiHeader> actualOaiHeaders = oaiHeaderCaptor.getAllValues().get(0);
        assertEquals("Exactly two OaiHeaders should have been removed from persistence.", 2, actualOaiHeaders.size());
        for (OaiHeader expected : oaiHeaders) {
            assertTrue("The OaiHeader '" + expected + "' has not been removed from persistence.",
                    actualOaiHeaders.contains(expected));
        }
    }

    /**
     * If receiving an incomplete METS XML that does not contain all required data such as a documentType, no
     * {@link ReportingDocumentMetadata} is persisted. Anyhow, the OaiHeader is removed from persistence to avoid
     * processing this document again as long as it has not been modified on the server.
     * 
     * @throws Exception
     */
    @Test
    public void rejectIncompleteMetsXML() throws Exception {

        // mock persistence returning one OaiHeader
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        Date datestamp31789 = dateFormat.parse("2016-09-29T16:04:39Z");
        String recordIdentifier31789 = "oai:example.org:qucosa:31789";
        OaiHeader qucosa31789Header = new OaiHeader(recordIdentifier31789, datestamp31789, false);

        List<OaiHeader> oaiHeaders = new LinkedList<>();
        oaiHeaders.add(qucosa31789Header);
        when(mockedPersistenceService.getOaiHeaders()).thenReturn(oaiHeaders);

        // mock mets dissemination service
        when(mockedHttpEntity.getContent()).thenAnswer(new Answer<InputStream>() {
            public InputStream answer(InvocationOnMock invocation) {
                return this.getClass().getResourceAsStream(METS_QUCOSA_31789_XML);
            }
        });

        runAndWait(metsHarvester);

        // assert that no ReportingDocumentMetadata object has been put to
        // persistence
        verify(mockedPersistenceService, atLeastOnce())
                .addOrUpdateReportingDocuments(reportingDocumentMetadataCaptor.capture());
        List<ReportingDocumentMetadata> actualReportingDoc = reportingDocumentMetadataCaptor.getAllValues().get(0);

        assertTrue("No ReportingDocumentMetadata object should have been persisted", actualReportingDoc.isEmpty());

        // assert OaiHeader has been removed from persistence
        verify(mockedPersistenceService, atLeastOnce()).removeOaiHeadersIfUnmodified(oaiHeaderCaptor.capture());
        List<OaiHeader> actualOaiHeaders = oaiHeaderCaptor.getAllValues().get(0);
        assertEquals("Exactly one OaiHeader should have been removed from persistence.", 1, actualOaiHeaders.size());
        assertEquals("The removed OaiHeader object is not equal to the expected object.", oaiHeaders.get(0),
                actualOaiHeaders.get(0));
    }

    /**
     * Make sure the date parser used by {@link MetsProcessor} can parse a date such as "2016-10-10T11:27:33+0200".
     * (There is no colon in the time zone, some parsers have problem with that)
     * 
     * @throws Exception
     */
    @Test
    public void parseISO8601DateInMetsXML() throws Exception {

        // mock persistence returning one OaiHeader
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        Date datestamp31790 = dateFormat.parse("2016-10-10T12:24:55Z");
        String recordIdentifier31790 = "oai:example.org:qucosa:31790";
        OaiHeader qucosa31790Header = new OaiHeader(recordIdentifier31790, datestamp31790, false);

        List<OaiHeader> oaiHeaders = new LinkedList<>();
        oaiHeaders.add(qucosa31790Header);
        when(mockedPersistenceService.getOaiHeaders()).thenReturn(oaiHeaders);

        // mock mets dissemination service
        when(mockedHttpEntity.getContent()).thenAnswer(new Answer<InputStream>() {
            public InputStream answer(InvocationOnMock invocation) {
                return this.getClass().getResourceAsStream(METS_QUCOSA_31790_XML);
            }
        });

        runAndWait(metsHarvester);

        // assert that ReportingDocumentMetadata has been parsed from mets dissemination and put to persistence
        verify(mockedPersistenceService, atLeastOnce())
                .addOrUpdateReportingDocuments(reportingDocumentMetadataCaptor.capture());
        List<ReportingDocumentMetadata> actualReportingDoc = reportingDocumentMetadataCaptor.getAllValues().get(0);

        assertEquals("Exactly one ReportingDocumentMetadata object should have been persisted", 1,
                actualReportingDoc.size());

        Date distributionDate = new Date(new DateTime("2016-10-10T11:27:33+0200").getMillis());
        ReportingDocumentMetadata expectedReportingDoc = new ReportingDocumentMetadata(recordIdentifier31790,
                "slub", "article", distributionDate, datestamp31790);

        assertEquals("The persisted ReportingDocumentMetadata object is not equal to the expected object.",
                expectedReportingDoc, actualReportingDoc.get(0));

        // assert OaiHeader has been removed from persistence
        verify(mockedPersistenceService, atLeastOnce()).removeOaiHeadersIfUnmodified(oaiHeaderCaptor.capture());
        List<OaiHeader> actualOaiHeaders = oaiHeaderCaptor.getAllValues().get(0);
        assertEquals("Exactly one OaiHeader should have been removed from persistence.", 1, actualOaiHeaders.size());
        assertEquals("The removed OaiHeader object is not equal to the expected object.", oaiHeaders.get(0),
                actualOaiHeaders.get(0));
    }

    /**
     * In case the METS dissemination service's http response does not contain a message entity, no
     * {@link ReportingDocumentMetadata} is written to persistence. A message is written to error log, containing the
     * recordIdentifier of the document that has not been processed successfully. The {@link OaiHeader} is removed from
     * persistence.
     * 
     * @throws Exception
     */
    @Test
    public void logEmptyResponseFromMetsDissemination() throws Exception {

        // mock persistence returning any OaiHeader
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        Date datestamp13 = dateFormat.parse("2015-12-17T16:03:17Z");
        final String recordIdentifier13 = "oai:example.org:qucosa:13";
        OaiHeader qucosa13Header = new OaiHeader(recordIdentifier13, datestamp13, false);

        List<OaiHeader> oaiHeaders = new LinkedList<>();
        oaiHeaders.add(qucosa13Header);
        when(mockedPersistenceService.getOaiHeaders()).thenReturn(oaiHeaders);

        // mock mets dissemination service, returning no message entity in its response
        when(mockedHttpResponse.getEntity()).thenReturn(null);

        // mock log appender
        Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        final Appender mockAppender = mock(Appender.class);
        when(mockAppender.getName()).thenReturn("MOCK");
        rootLogger.addAppender(mockAppender);

        runAndWait(metsHarvester);

        // assert that a message has been written to error log, containing the recordIdentifier of the document
        verify(mockAppender).doAppend(argThat(new ArgumentMatcher() {
            @Override
            public boolean matches(final Object argument) {

                Level logLevel = ((LoggingEvent) argument).getLevel();
                String logMsg = ((LoggingEvent) argument).getFormattedMessage();

                return (logLevel == Level.ERROR)
                        && (logMsg.contains(MetsProcessor.ERROR_MSG_EMPTY_RESPONSE_FROM_METS_DISSEMINATION_SERVICE))
                        && (logMsg.contains(recordIdentifier13));
            }
        }));

        // assert no ReportingDocumentMetadata has been put to persistence
        verify(mockedPersistenceService, atLeastOnce())
                .addOrUpdateReportingDocuments(reportingDocumentMetadataCaptor.capture());
        List<ReportingDocumentMetadata> actualReportingDoc = reportingDocumentMetadataCaptor.getAllValues().get(0);

        assertTrue("No ReportingDocumentMetadata object should have been persisted", actualReportingDoc.isEmpty());

        // assert OaiHeader has been removed from persistence
        verify(mockedPersistenceService, atLeastOnce()).removeOaiHeadersIfUnmodified(oaiHeaderCaptor.capture());
        List<OaiHeader> actualOaiHeaders = oaiHeaderCaptor.getAllValues().get(0);
        assertEquals("Exactly one OaiHeader should have been removed from persistence.", 1, actualOaiHeaders.size());
        assertEquals("The removed OaiHeader object is not equal to the expected object.", oaiHeaders.get(0),
                actualOaiHeaders.get(0));
    }

    /**
     * In case the METS dissemination service's http response is 404, no {@link ReportingDocumentMetadata} is written to
     * persistence. A message is written to error log, containing the recordIdentifier of the document that has not been
     * processed successfully. The {@link OaiHeader} is removed from persistence.
     * 
     * @throws Exception
     */
    @Test
    public void logHttpErrorResponseFromMetsDissemination() throws Exception {

        // mock persistence returning any OaiHeader
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        Date datestamp13 = dateFormat.parse("2015-12-17T16:03:17Z");
        final String recordIdentifier13 = "oai:example.org:qucosa:13";
        OaiHeader qucosa13Header = new OaiHeader(recordIdentifier13, datestamp13, false);

        List<OaiHeader> oaiHeaders = new LinkedList<>();
        oaiHeaders.add(qucosa13Header);
        when(mockedPersistenceService.getOaiHeaders()).thenReturn(oaiHeaders);

        // mock mets dissemination service, returning HTTP 404
        when(mockedStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
        when(mockedStatusLine.getReasonPhrase()).thenReturn("Not Found");

        // mock log appender
        Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        final Appender mockAppender = mock(Appender.class);
        when(mockAppender.getName()).thenReturn("MOCK");
        rootLogger.addAppender(mockAppender);

        runAndWait(metsHarvester);

        // assert that a message has been written to error log, containing the recordIdentifier of the document
        verify(mockAppender).doAppend(argThat(new ArgumentMatcher() {
            @Override
            public boolean matches(final Object argument) {

                Level logLevel = ((LoggingEvent) argument).getLevel();
                String logMsg = ((LoggingEvent) argument).getFormattedMessage();

                return (logLevel == Level.ERROR) && (logMsg.contains(MetsProcessor.ERROR_MSG_UNEXPECTED_HTTP_RESPONSE))
                        && (logMsg.contains(recordIdentifier13));
            }
        }));

        // assert no ReportingDocumentMetadata has been put to persistence
        verify(mockedPersistenceService, atLeastOnce())
                .addOrUpdateReportingDocuments(reportingDocumentMetadataCaptor.capture());
        List<ReportingDocumentMetadata> actualReportingDoc = reportingDocumentMetadataCaptor.getAllValues().get(0);

        assertTrue("No ReportingDocumentMetadata object should have been persisted", actualReportingDoc.isEmpty());

        // assert OaiHeader has been removed from persistence
        verify(mockedPersistenceService, atLeastOnce()).removeOaiHeadersIfUnmodified(oaiHeaderCaptor.capture());
        List<OaiHeader> actualOaiHeaders = oaiHeaderCaptor.getAllValues().get(0);
        assertEquals("Exactly one OaiHeader should have been removed from persistence.", 1, actualOaiHeaders.size());
        assertEquals("The removed OaiHeader object is not equal to the expected object.", oaiHeaders.get(0),
                actualOaiHeaders.get(0));
    }

    @Before
    public void setUp() throws Exception {

        MockitoAnnotations.initMocks(this);
        mockedPersistenceService = mock(PersistenceService.class);

        mockedHttpClient = mock(CloseableHttpClient.class);
        mockedHttpResponse = mock(CloseableHttpResponse.class);
        when(mockedHttpClient.execute(any(HttpUriRequest.class))).thenReturn(mockedHttpResponse);
        mockedStatusLine = mock(StatusLine.class);
        when(mockedHttpResponse.getStatusLine()).thenReturn(mockedStatusLine);
        when(mockedStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        mockedHttpEntity = mock(HttpEntity.class);
        when(mockedHttpResponse.getEntity()).thenReturn(mockedHttpEntity);

        HashMap<String, String> prefMap = new HashMap<>();
        prefMap.put("mets", "http://www.loc.gov/METS/");
        prefMap.put("slub", "http://slub-dresden.de/");
        prefMap.put("v3", "http://www.loc.gov/mods/v3");

        metsHarvester = new MetsProcessor(new URI("http://localhost:8080/mets/"), POLLING_INTERVAL,
                MINIMUM_WAITTIME_BETWEEN_TWO_REQUESTS, prefMap, mockedPersistenceService, mockedHttpClient);

    }

    private void runAndWait(TerminateableRunnable runnable) throws InterruptedException {
        Thread thread = new Thread(runnable);
        thread.start();
        TimeUnit.MILLISECONDS.sleep(SLEEP_TIME_RUN_AND_WAIT);
        runnable.terminate();
        thread.join();
    }
}
