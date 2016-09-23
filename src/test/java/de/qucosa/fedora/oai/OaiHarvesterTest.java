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

package de.qucosa.fedora.oai;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import de.qucosa.persistence.PersistenceService;
import de.qucosa.util.TerminateableRunnable;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OaiHarvesterTest {

    private static final boolean FCREPO3_COMPATIBILITY_MODE = false;

    private static final String OAI_LIST_IDENTIFIERS_XML = "/oai/listIdentifiers.xml";
    private static final String OAI_RESUMPTION_TOKEN_XML = "/oai/resumptionToken.xml";
    private static final String OAI_ERROR_NO_RECORDS_MATCH_XML = "/oai/errorNoRecordsMatch.xml";
    private static final String OAI_ERROR_BAD_RESUMPTION_TOKEN_XML = "/oai/errorBadResumptionToken.xml";
    private static final String OAI_ERROR_MULTIPLE_ERRORS_XML = "/oai/multipleErrors.xml";
    private static final String OAI_EMPTY_RESUMPTION_TOKEN_XML = "/oai/emptyResumptionToken.xml";
    private static final String OAI_IDENTIFIERS_TO_FILTER_XML = "/oai/ListIdentifiersToFilter.xml";

    private EmbeddedHttpHandler embeddedHttpHandler;
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
        List<OaiHeader> actualOaiHeaders = (List<OaiHeader>) oaiHeaderCaptor.getValue();
        assertEquals("Wrong number of headers, filter does not work.", 6, actualOaiHeaders.size());

        List<String> actualIDs = new LinkedList<>();
        for (OaiHeader header : actualOaiHeaders) {
            actualIDs.add(header.getRecordIdentifier());
        }

        String[] expectedIDs = {"oai:example.org:qucosa:1", "oai:example.org:qucosa:22", "oai:example.org:qucosa:333",
                "oai:example.org:qucosa:4444", "oai:example.org:qucosa:55555", "oai:example.org:qucosa:666666"};

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
    public void createGETfromNullLastOaiRunResult() throws Exception {

        when(mockedPersistenceService.getLastOaiRunResult()).thenReturn(null);

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
        when(mockedPersistenceService.getLastOaiRunResult()).thenReturn(lastRun);

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
        when(mockedPersistenceService.getLastOaiRunResult()).thenReturn(lastRun);

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
        when(mockedPersistenceService.getLastOaiRunResult()).thenReturn(lastRun);

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
     * persistence layer. Assert, that this {@link OaiRunResult}'s
     * nextFromTimestamp is {@code null}
     *
     * @throws Exception
     */
    @Test
    public void createOaiRunResultFromResumptionToken() throws Exception {

        when(mockedPersistenceService.getLastOaiRunResult()).thenReturn(null);

        embeddedHttpHandler.resourcePath = OAI_RESUMPTION_TOKEN_XML;
        runAndWait(oaiHarvester);

        ArgumentCaptor<OaiRunResult> captor = ArgumentCaptor.forClass(OaiRunResult.class);
        verify(mockedPersistenceService).storeOaiRunResult(captor.capture());
        OaiRunResult actualOaiRunResult = captor.getValue();

        assertNotNull("No OaiRunResult was returned by persistence layer", actualOaiRunResult);

        Date expectedResponseDate = parseDateTime("2014-06-08T11:43:00Z");
        String expectedResumptionToken = "111111111111111";
        Date expectedExpirationDate = parseDateTime("2014-06-09T18:34:15Z");

        // compare all of OaiRunResults values except timestampOfRun which is
        // generated by the harvester at runtime
        assertEquals(expectedResponseDate, actualOaiRunResult.getResponseDate());
        assertEquals(expectedResumptionToken, actualOaiRunResult.getResumptionToken());
        assertEquals(expectedExpirationDate, actualOaiRunResult.getResumptionTokenExpirationDate());
        assertEquals("There must not be any nextFromTimestamp. On error, we need to request all data, "
                + "not excluding anything by from-parameter.", null, actualOaiRunResult.getNextFromTimestamp());
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

        OaiRunResult lastOaiRunResult = new OaiRunResult(timestampLastRun, responseDate, resumptionToken,
                resumptionTokenExpirationDate, nextFromTimestamp);

        when(mockedPersistenceService.getLastOaiRunResult()).thenReturn(lastOaiRunResult);

        embeddedHttpHandler.resourcePath = OAI_RESUMPTION_TOKEN_XML;
        runAndWait(oaiHarvester);

        ArgumentCaptor<OaiRunResult> captor = ArgumentCaptor.forClass(OaiRunResult.class);
        verify(mockedPersistenceService).storeOaiRunResult(captor.capture());
        OaiRunResult actualOaiRunResult = captor.getValue();

        assertNotNull("No OaiRunResult was returned by persistence layer", actualOaiRunResult);

        Date expectedResponseDate = parseDateTime("2014-06-08T11:43:00Z");
        String expectedResumptionToken = "111111111111111";
        Date expectedExpirationDate = parseDateTime("2014-06-09T18:34:15Z");

        // compare all of OaiRunResults values except timestampOfRun which is
        // generated by the harvester at runtime
        assertEquals(expectedResponseDate, actualOaiRunResult.getResponseDate());
        assertEquals(expectedResumptionToken, actualOaiRunResult.getResumptionToken());
        assertEquals(expectedExpirationDate, actualOaiRunResult.getResumptionTokenExpirationDate());
        assertEquals("The nextFromTimestamp should have been copied from the last OaiRunResult.", nextFromTimestamp,
                actualOaiRunResult.getNextFromTimestamp());
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
        when(mockedPersistenceService.getLastOaiRunResult()).thenReturn(lastRun);

        // use any "normal" response without resumptionToken
        embeddedHttpHandler.resourcePath = OAI_EMPTY_RESUMPTION_TOKEN_XML;
        runAndWait(oaiHarvester);

        ArgumentCaptor<OaiRunResult> captor = ArgumentCaptor.forClass(OaiRunResult.class);
        verify(mockedPersistenceService).storeOaiRunResult(captor.capture());
        OaiRunResult actualOaiRunResult = captor.getValue();

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

        when(mockedPersistenceService.getLastOaiRunResult()).thenReturn(null);

        // use any "normal" response without resumptionToken
        embeddedHttpHandler.resourcePath = OAI_LIST_IDENTIFIERS_XML;
        runAndWait(oaiHarvester);

        ArgumentCaptor<OaiRunResult> captor = ArgumentCaptor.forClass(OaiRunResult.class);
        verify(mockedPersistenceService).storeOaiRunResult(captor.capture());
        OaiRunResult actualOaiRunResult = captor.getValue();

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

        OaiRunResult lastOaiRunResult = new OaiRunResult(timestampLastRun, responseDate, resumptionToken,
                resumptionTokenExpirationDate, nextFromTimestamp);
        when(mockedPersistenceService.getLastOaiRunResult()).thenReturn(lastOaiRunResult);

        // use any "normal" response without resumptionToken
        embeddedHttpHandler.resourcePath = OAI_LIST_IDENTIFIERS_XML;
        runAndWait(oaiHarvester);

        ArgumentCaptor<OaiRunResult> captor = ArgumentCaptor.forClass(OaiRunResult.class);
        verify(mockedPersistenceService).storeOaiRunResult(captor.capture());
        OaiRunResult actualOaiRunResult = captor.getValue();

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

        OaiRunResult lastOaiRunResult = new OaiRunResult(timestampLastRun, responseDate, resumptionToken,
                resumptionTokenExpirationDate, nextFromTimestamp);
        when(mockedPersistenceService.getLastOaiRunResult()).thenReturn(lastOaiRunResult);

        embeddedHttpHandler.resourcePath = OAI_EMPTY_RESUMPTION_TOKEN_XML;
        runAndWait(oaiHarvester);

        ArgumentCaptor<OaiRunResult> captor = ArgumentCaptor.forClass(OaiRunResult.class);
        verify(mockedPersistenceService).storeOaiRunResult(captor.capture());
        OaiRunResult actualOaiRunResult = captor.getValue();

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

        ArgumentCaptor<OaiRunResult> captor = ArgumentCaptor.forClass(OaiRunResult.class);
        verify(mockedPersistenceService).storeOaiRunResult(captor.capture());
        OaiRunResult actualOaiRunResult = captor.getValue();

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
        when(mockedPersistenceService.getLastOaiRunResult()).thenReturn(initialOaiRunResult);

        embeddedHttpHandler.resourcePath = OAI_ERROR_BAD_RESUMPTION_TOKEN_XML;
        runAndWait(oaiHarvester);

        ArgumentCaptor<OaiRunResult> captor = ArgumentCaptor.forClass(OaiRunResult.class);
        verify(mockedPersistenceService).storeOaiRunResult(captor.capture());
        OaiRunResult actualOaiRunResult = captor.getValue();
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
        when(mockedPersistenceService.getLastOaiRunResult()).thenReturn(initialOaiRunResult);

        embeddedHttpHandler.resourcePath = OAI_ERROR_MULTIPLE_ERRORS_XML;
        runAndWait(oaiHarvester);

        ArgumentCaptor<OaiRunResult> captor = ArgumentCaptor.forClass(OaiRunResult.class);
        verify(mockedPersistenceService).storeOaiRunResult(captor.capture());
        OaiRunResult actualOaiRunResult = captor.getValue();
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
    /*---- Begin test logic for cleanup of OaiRunResult history ----*/

    /**
     * Test that the cleanup of {@link OaiRunResult}s in persistence layer is
     * triggered after a successful run.
     *
     * @throws Exception
     */
    @Test
    public void cleanupOaiRunResultHistoryAfterSuccessfulRun() throws Exception {

        // new OaiHarvester that keeps a history of 1 day
        Duration historyLength = Duration.standardDays(1);
        oaiHarvester = new OaiHarvesterBuilder(new URI("http://localhost:8000/fedora/oai"), mockedPersistenceService)
                .setPollingInterval(Duration.standardSeconds(1)).setOaiRunResultHistory(historyLength).build();

        // just let the harvester do something to have one successful loop
        // and invoke the cleanup (the Oai data provider's response is ignored)
        embeddedHttpHandler.resourcePath = OAI_RESUMPTION_TOKEN_XML;
        Date beforeHarvesterRuns = now();
        runAndWait(oaiHarvester);
        Date afterHarvesterRuns = now();

        ArgumentCaptor<Date> captor = ArgumentCaptor.forClass(Date.class);
        verify(mockedPersistenceService).cleanupOaiRunResults(captor.capture());
        Date cleanupDate = captor.getValue();

        // testing the cleanupDate is tricky since it is calculated at the time
        // of testing. we want to keep a history of 1 day: cleanupDate=now-1day
        assertTrue(cleanupDate.after(new Date(beforeHarvesterRuns.getTime() - historyLength.getMillis())));
        assertTrue(cleanupDate.before(new Date(afterHarvesterRuns.getTime() - historyLength.getMillis())));
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

    /*---- End test logic for cleanup of OaiRunResult history ----*/

    @Before
    public void setUp() throws Exception {

        MockitoAnnotations.initMocks(this);
        mockedPersistenceService = mock(PersistenceService.class);
        createOaiHarvester();
        setupHttpServer();
    }

    @After
    public void stopHttpServer() {
        httpServer.stop(1);
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

//    @NonNull
    private Date parseDateTime(String timestamp) throws IllegalArgumentException {
        if (StringUtils.isBlank(timestamp)) {
            throw new IllegalArgumentException("timestamp must not be null or empty");
        }
        return DatatypeConverter.parseDateTime(timestamp).getTime();
    }

    class EmbeddedHttpHandler implements HttpHandler {

        public int httpStatusCode = 200;
        public URI lastRequestUri;
        public String resourcePath;

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            lastRequestUri = exchange.getRequestURI();
            exchange.sendResponseHeaders(httpStatusCode, 0);
            IOUtils.copy(this.getClass().getResourceAsStream(resourcePath), exchange.getResponseBody());
            exchange.getResponseBody().close();
        }
    }

}
