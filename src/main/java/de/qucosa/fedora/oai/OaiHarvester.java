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

package de.qucosa.fedora.oai;

import de.qucosa.persistence.PersistenceException;
import de.qucosa.persistence.PersistenceService;
import de.qucosa.util.TerminateableRunnable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.ws.rs.core.UriBuilder;
import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OaiHarvester extends TerminateableRunnable {

    private static final String OAI_PMH_ERROR_NO_RECORDS_MATCH = "noRecordsMatch";
    private static final String OAI_PMH_ERROR_BAD_RESUMPTION_TOKEN = "badResumptionToken";

    // TODO add to properties file?
    private static final long SERVER_TIME_DIFFERENCE_WARNING_MILLIS = TimeUnit.MINUTES.toMillis(2);

    // format should be yyyy-MM-dd'T'HH:mm:ss'Z' with 'Z' in the end.
    // Fedora Commons 3 has a bug in processing the from
    // parameter: if the 'Z' is present, the specified day is ignored and
    // results start from the next day.
    // In any case, the time part is always ignored as if a request contained a
    // date only.
    // example1: request from=2016-01-01T13:20:00 returns results starting from
    // 2016-01-01T00:00:00Z.
    // example2: request from=2016-01-01T13:20:00Z returns results starting from
    // 2016-01-02T00:00:00Z.
    private static final SimpleDateFormat FCREPO3_TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private static final SimpleDateFormat DEFAULT_URI_TIMESTAMP_FORMAT = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static final OaiRunResult EMPTY_OAI_RUN_RESULT = new OaiRunResult();
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final OaiHeaderFilter oaiHeaderFilter;
    /**
     * Time span to keep OaiRunResults in persistence.
     */
    private final Duration oaiRunResultHistoryLength;
    private final PersistenceService persistenceService;
    private final Duration pollInterval;
    private final Duration minimumWaittimeBetweenTwoRequests;// = Duration.millis(10);
    private final URI uri;
    private final SimpleDateFormat uriTimestampFormat;
    private final boolean useFC3CompatibilityMode;
    private List<OaiHeader> harvestedHeaders = new LinkedList<>();
    
    private final CloseableHttpClient httpClient;
    
    /**
     * If true, the last run was not successful and no OaiRunResult has been written to persistence.
     */
    private boolean lastRunResultedInError = false; 

    // TODO constructor does no checks now, everything done by builder.
    protected OaiHarvester(URI harvestingUri, Duration pollInterval, Duration minimumWaittimeBetweenTwoRequests, 
                           OaiHeaderFilter oaiHeaderFilter, PersistenceService persistenceService, 
                           Duration oaiRunResultHistoryLength, boolean useFC3CompatibilityMode,
                           CloseableHttpClient httpClient) {

        this.uri = harvestingUri;
        this.pollInterval = pollInterval;
        this.minimumWaittimeBetweenTwoRequests = minimumWaittimeBetweenTwoRequests;
        this.oaiRunResultHistoryLength = oaiRunResultHistoryLength;
        this.persistenceService = persistenceService;
        this.oaiHeaderFilter = oaiHeaderFilter;
        this.useFC3CompatibilityMode = useFC3CompatibilityMode;
        this.uriTimestampFormat = (useFC3CompatibilityMode) ? FCREPO3_TIMESTAMP_FORMAT : DEFAULT_URI_TIMESTAMP_FORMAT;
        this.httpClient = httpClient;
    }

    @Override
    public void run() {
        PeriodFormatter formatter = new PeriodFormatterBuilder().printZeroIfSupported().appendHours()
                .appendSuffix("hr", "hrs").appendSeparator(":").minimumPrintedDigits(2).appendMinutes()
                .appendSuffix("min", "min").appendSeparator(":").appendSecondsWithOptionalMillis()
                .appendSuffix("sec", "sec").toFormatter();
        String interval = formatter.print(this.pollInterval.toPeriod());
        this.logger.info("Harvesting URL: {} every {}", this.uri.toASCIIString(), interval);

        try {
            harvestLoop();
        } catch (Exception e) {
            logger.error(ensureMessage(e));
        }
    }

    private void harvestLoop() {
        do {
            final OaiRunResult lastRun = getLastrunParameters();
            if (waitForNextRun(lastRun)) {

                final OaiRunResult currentRun = harvest(lastRun);

                if (currentRun.hasTimestampOfRun()) {

                    harvestedHeaders = oaiHeaderFilter.filterOaiHeaders(harvestedHeaders);

                    try {
                        persistenceService.addOrUpdateOaiHeaders(new LinkedList<>(harvestedHeaders));
                        harvestedHeaders = new LinkedList<>();

                        try {
                            persistenceService.storeOaiRunResult(currentRun);
                        } catch (PersistenceException exception) {
                            logger.error("The status of the current run could not be persisted, "
                                    + "the previous OaiRunResult remains the most recent one.", exception);
                        }
                        lastRunResultedInError = false;

                    } catch (PersistenceException exception) {
                        logger.error("Harvested headers could not be persisted. This run was not successful, "
                                + "the previous OaiRunResult is still the most recent one. ", exception);
                    }
                } else {
                    lastRunResultedInError = true;
                }
                cleanupOaiRunResultsInPersistence(currentRun);
            }
        } while (isRunning());
    }

    private void cleanupOaiRunResultsInPersistence(OaiRunResult currentOaiRunResult) {

        Date currentRun = currentOaiRunResult.getTimestampOfRun();
        if (currentRun != null) {

            Date lastRunToKeep = new Date(currentRun.getTime() - oaiRunResultHistoryLength.getMillis());

            try {
                persistenceService.cleanupOaiRunResults(lastRunToKeep);
            } catch (PersistenceException exception) {
                logger.warn("Could not cleanup OaiRunResults in persistence layer.", exception);
            }

        } else {
            logger.debug("The current harvesting run was not successful. "
                    + "Skipping cleanup of OaiRunResults in persistence layer.");
        }
    }

    /**
     * See file /doc/HarvesterResumptionLogic.xlsx, sheet "waitForNextRun logic" for details on how to calculate
     * the time to wait. 
     * 
     * @param lastrun
     * @return
     */
    //TODO: write UnitTests for waitForNextRun logic
    private boolean waitForNextRun(OaiRunResult lastrun) {
        Date start = now();
        Date timestampLastRun = lastrun.getTimestampOfRun();
        long waitTime = pollInterval.getMillis();

        if (timestampLastRun == null && !lastRunResultedInError) {
            // case 1: very first run of harvester, start immediately
            waitTime = 0;
        
        } else if (timestampLastRun != null && !lastrun.hasResumptionToken() && !lastRunResultedInError) {
            if (!timestampLastRun.after(start)) {
                // case 2: next run is at timestampLastRun + pollInterval 
                waitTime = pollInterval.getMillis() - (start.getTime() - timestampLastRun.getTime());

            } else {
              // exception: something is wrong with the clock
              waitTime = pollInterval.getMillis();
              logger.error("The timestamp of the last run seems to be in the future. "
                      + "Either the persistence layer is corrupted or the local servers clock travels in time...");
            }
        } else if (lastrun.hasResumptionToken() && !lastRunResultedInError) {
            // case 3: we have a resumption token, process OAI service provider's paginated response

            waitTime = minimumWaittimeBetweenTwoRequests.getMillis();
        } else {
            // case 4: on any other case
            waitTime = pollInterval.getMillis();
        }
        
        try {
            TimeUnit.MILLISECONDS.sleep(waitTime);
            return true;
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for next OAI run: {}", e.getMessage());
            return false;
        }
    }

    private OaiRunResult harvest(OaiRunResult lastRunResult) {
        Date startTimeOfCurrentRun = now();
        URI uri = buildOaiRequestURI(lastRunResult);

        final Date lastRunTimestamp = lastRunResult.getTimestampOfRun();
        if (lastRunTimestamp != null)
            logger.debug("Last OAI run was at {}", lastRunTimestamp);

        logger.debug("Requesting {}", uri.toASCIIString());

        HttpGet httpGet = new HttpGet(uri);
        OaiRunResult result = EMPTY_OAI_RUN_RESULT;
        
        try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
            if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                HttpEntity httpEntity = httpResponse.getEntity();
                if (httpEntity != null) {
                    // TODO nice-to-have validate httpEntity.getContent() against schema - is it valid OAI-PMH?
                    result = handleXmlResult(httpEntity.getContent(), startTimeOfCurrentRun, lastRunResult);
                } else {
                    logger.error("Got empty response from OAI service.");
                }
            } else {
                logger.error("Unexpected OAI service response: {} {}", httpResponse.getStatusLine().getStatusCode(),
                        httpResponse.getStatusLine().getReasonPhrase());
            }
        } catch (Exception ex) {
            logger.error(ensureMessage(ex));
        }
        return result;
    }

    private String ensureMessage(Exception ex) {
        String message = ex.getMessage();
        if (message == null || message.isEmpty()) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            message = sw.toString();
        }
        return message;
    }

    private Date now() {
        return Calendar.getInstance().getTime();
    }

    private OaiRunResult getLastrunParameters() {

        OaiRunResult result = persistenceService.getLastOaiRunResult();

        if (result == null) {
            result = EMPTY_OAI_RUN_RESULT;
        }

        return result;
    }

    /**
     * Build ListIdentifiers URI to request OAI data provider, using the
     * resumptionToken from lastrun (if there was one) or from-parameter
     *
     * @param lastrun
     * @return
     */
    private URI buildOaiRequestURI(OaiRunResult lastrun) {
        UriBuilder builder = UriBuilder.fromUri(uri).queryParam("verb", "ListIdentifiers");

        if (lastrun.hasResumptionToken()) {
            builder.queryParam("resumptionToken", lastrun.getResumptionToken());
        } else {
            builder.queryParam("metadataPrefix", "oai_dc");

            if (lastrun.hasNextFromTimestamp()) {
                builder.queryParam("from", uriTimestampFormat.format(lastrun.getNextFromTimestamp()));
            }
        }

        return builder.build();
    }

    /**
     * See file /doc/HarvesterResumptionLogic.xlsx, sheet "OaiRunResult processing" for details on how the new 
     * {@link OaiRunResult} is created, based on lastRunResult, resumptionToken flow control (and OAI-PMH errors) 
     * 
     * @param content
     * @param startTimeOfCurrentRun
     * @param lastRunResult
     * @return
     * @throws ParserConfigurationException
     * @throws IOException
     * @throws SAXException
     * @throws XPathExpressionException
     * @throws IllegalArgumentException
     */
    private OaiRunResult handleXmlResult(InputStream content, Date startTimeOfCurrentRun, OaiRunResult lastRunResult)
            throws ParserConfigurationException, IOException, SAXException, XPathExpressionException,
            IllegalArgumentException {

        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        Document document = documentBuilderFactory.newDocumentBuilder().parse(content);

        Map<String, String> oaiErrorsFound = extractOaiErrors(document);
        Date currentResponseDate = extractResponseDate(document, startTimeOfCurrentRun);
        String currentResumptionToken = extractResumptionToken(document);
        Date currentResumptionTokenExpirationDate = extractResumptionTokenExpirationDate(document);
        extractOaiHeaderElements(document);

        // handle OAI-PMH errors and resumptionToken flow control
        // (see also
        // https://www.openarchives.org/OAI/openarchivesprotocol.html#FlowControl)
        // and doc/HarvesterResumptionLogic.xlsx for examples
        Date nextFromTimestamp;
        if (oaiErrorsFound.isEmpty()) {

            if (currentResumptionToken == null) {
                // no current resumptionToken present

                if (StringUtils.isBlank(lastRunResult.getResumptionToken())) {
                    // regular behavior.
                    // last resumptionToken is null or empty
                    nextFromTimestamp = startTimeOfCurrentRun;

                } else {
                    // This is against the specification.
                    if (useFC3CompatibilityMode) {
                        // anyhow, it seems that Fedora Commons 3 has a bug
                        // not closing an paginated result with an empty
                        // resumption token... so in compatibility mode, this is
                        // regular behavior.
                        nextFromTimestamp = startTimeOfCurrentRun;

                    } else {
                        logger.error(
                                "Last run had a resumption token, current response did not contain an (empty) resumption "
                                        + "token. This is against the specification. Next harvesting loop will use the "
                                        + "nextFromTimestamp that had been backed up from previous run: '{}'",
                                lastRunResult.getNextFromTimestamp());
                        nextFromTimestamp = lastRunResult.getNextFromTimestamp();
                    }
                }

            } else if (StringUtils.isBlank(currentResumptionToken)) {
                // current resumptionToken exists, but is empty, indicating the
                // end of a paginated list

                if (StringUtils.isBlank(lastRunResult.getResumptionToken())) {
                    // last resumptionToken was empty of null. This is against
                    // the specification.
                    logger.error(
                            "Current response contains an empty resumption token but the last response did not contain "
                                    + "a resumptionToken. This is against the specification. Next harvesting loop will use the "
                                    + "nextFromTimestamp that had been backed up from previous run: '{}'",
                            lastRunResult.getNextFromTimestamp());
                    nextFromTimestamp = lastRunResult.getNextFromTimestamp();

                } else {
                    // regular behavior
                    // last resumptionToken had a 'useful' value
                    nextFromTimestamp = startTimeOfCurrentRun;
                }

            } else {
                // regular behavior
                // current resumptionToken has 'useful' value
                // backup the last nextFromTimestamp to be able to re-request
                // the last GET request that contained a from parameter or
                // request all data if there was never a GET request with a from
                // parameter (first run)
                nextFromTimestamp = lastRunResult.getNextFromTimestamp();
            }

        } else {
            // OAI-PMH error(s) found
            // if multiple errors occur, they may be handled separately. For
            // now, we ignore this
            if (oaiErrorsFound.containsKey(OAI_PMH_ERROR_NO_RECORDS_MATCH)) {
                // this is not seen as an error.
                nextFromTimestamp = startTimeOfCurrentRun;
                logger.debug(
                        "OAI data provider sent an noRecordsMatch error. This is not considered an error, "
                                + "maybe we poll to frequently. Error details: '{}'",
                        oaiErrorsFound.get(OAI_PMH_ERROR_NO_RECORDS_MATCH));

            } else if (oaiErrorsFound.containsKey(OAI_PMH_ERROR_BAD_RESUMPTION_TOKEN)) {
                
                //FIXME: if this happens in the first harvest run while looping over the paginated request of the initial run, there is no nextFromTimestamp! lastRunResult.getNextFromTimestamp() might be null ??
                nextFromTimestamp = lastRunResult.getNextFromTimestamp();
                logger.warn(
                        "Last resumption token was invalid or unknown to server. "
                                + "Next harvesting loop will use the nextFromTimestamp that had been backed up from previous run: '{}'",
                        lastRunResult.getNextFromTimestamp());

            } else {
                nextFromTimestamp = lastRunResult.getNextFromTimestamp();
                logger.error(
                        "OAI data provider sent an error that cant be handled. "
                                + "Next harvesting loop will use the nextFromTimestamp that had been backed up from previous run: '{}'. "
                                + "Unknown errors are: {}",
                        lastRunResult.getNextFromTimestamp(), oaiErrorsFound.toString());
            }
        }

        return new OaiRunResult(startTimeOfCurrentRun, currentResponseDate, currentResumptionToken,
                currentResumptionTokenExpirationDate, nextFromTimestamp);
    }

    private Map<String, String> extractOaiErrors(Document document) throws XPathExpressionException {

        Map<String, String> oaiErrorsFound = new HashMap<>();
        XPath xPath = XPathFactory.newInstance().newXPath();

        XPathExpression xSelectHeader = xPath.compile("//error");
        NodeList errorNodes = (NodeList) xSelectHeader.evaluate(document, XPathConstants.NODESET);

        for (int i = 0; i < errorNodes.getLength(); i++) {
            Node singleErrorNode = errorNodes.item(i);

            if (singleErrorNode.getNodeType() == Node.ELEMENT_NODE) {

                Element errorElement = (Element) singleErrorNode;
                String errorCode = errorElement.getAttribute("code");
                String errorMsg = errorElement.getTextContent();
                oaiErrorsFound.put(errorCode, errorMsg);
            }
        }

        if (!oaiErrorsFound.isEmpty()) {
            logger.debug("Response contained OAI errors: ", oaiErrorsFound.toString());
        }

        return oaiErrorsFound;
    }

    private Date extractResumptionTokenExpirationDate(Document document) throws XPathExpressionException {
        XPath xPath = XPathFactory.newInstance().newXPath();
        XPathExpression xSelectExpirationDate = xPath.compile("//resumptionToken/@expirationDate");
        String resumptionExpiration = (String) xSelectExpirationDate.evaluate(document, XPathConstants.STRING);
        Date resumptionTokenExpirationDate = parseNullableDateTime(resumptionExpiration);
        return resumptionTokenExpirationDate;
    }

    /**
     * OAI-PMH flow control resumptionTokens may have a value, be empty or are
     * not existent. Each case has a different meaning.
     *
     * @param document
     * @return either {@code null} if there is no resumptionToken, or empty
     * String "" if the document contains an empty resumptionToken or a
     * String with length() > 0 containing the resumptionTokens's value.
     * @throws XPathExpressionException
     */
    private String extractResumptionToken(Document document) throws XPathExpressionException {
        String resumptionToken = null;
        XPath xPath = XPathFactory.newInstance().newXPath();

        Node node = (Node) xPath.evaluate("//resumptionToken", document, XPathConstants.NODE);
        if (node != null) {
            XPathExpression xSelectResumptionToken = xPath.compile("//resumptionToken");
            resumptionToken = (String) xSelectResumptionToken.evaluate(document, XPathConstants.STRING);
            if (resumptionToken != null) {
                resumptionToken = resumptionToken.trim();
            }
        }

        return resumptionToken;
    }

    private Date extractResponseDate(Document document, Date startTimeOfCurrentRun) throws XPathExpressionException {
        XPath xPath = XPathFactory.newInstance().newXPath();
        XPathExpression xSelectResponseDate = xPath.compile("//responseDate");
        String responseDateString = (String) xSelectResponseDate.evaluate(document, XPathConstants.STRING);
        Date responseDate = DatatypeConverter.parseDateTime(responseDateString).getTime();

        long serverTimeDifferenceMillis = Math.abs(responseDate.getTime() - startTimeOfCurrentRun.getTime());
        if (serverTimeDifferenceMillis > SERVER_TIME_DIFFERENCE_WARNING_MILLIS) {
            logger.warn("Local server time and remote server time have a huge difference of "
                    + DurationFormatUtils.formatDuration(serverTimeDifferenceMillis, "d 'days,' HH'h':mm'm':ss's'"));
        }
        return responseDate;
    }

    /**
     * Use XPath to generate {@link OaiHeader} objects from the document and add
     * them to {@link #harvestedHeaders}.
     *
     * @param document
     * @throws XPathExpressionException
     */
    private void extractOaiHeaderElements(Document document) throws XPathExpressionException {
        XPath xPath = XPathFactory.newInstance().newXPath();

        XPathExpression xSelectHeader = xPath.compile("//header");
        NodeList headerNodes = (NodeList) xSelectHeader.evaluate(document, XPathConstants.NODESET);
        logger.debug("{} header elements in OAI result", headerNodes.getLength());

        for (int i = 0; i < headerNodes.getLength(); i++) {
            Node singleHeaderNode = headerNodes.item(i);

            if (singleHeaderNode.getNodeType() == Node.ELEMENT_NODE) {
                Element headerElement = (Element) singleHeaderNode;

                boolean statusIsDeleted = headerElement.getAttribute("status").equalsIgnoreCase("deleted");
                String recordIdentifier = headerElement.getElementsByTagName("identifier").item(0).getChildNodes()
                        .item(0).getNodeValue();
                String datestampString = headerElement.getElementsByTagName("datestamp").item(0).getChildNodes().item(0)
                        .getNodeValue();
                Date datestampDate = DatatypeConverter.parseDateTime(datestampString).getTime();
                LinkedList<String> setSpecList = new LinkedList<>();

                NodeList setSpecNodes = headerElement.getElementsByTagName("setSpec");
                for (int j = 0; j < setSpecNodes.getLength(); j++) {
                    String setSpec = setSpecNodes.item(j).getChildNodes().item(0).getNodeValue();
                    setSpecList.add(setSpec);
                }
                OaiHeader receivedHeader = new OaiHeader(recordIdentifier, datestampDate, setSpecList, statusIsDeleted);
                // OaiHeader receivedHeader = new OaiHeader(recordIdentifier,
                // datestampDate, statusIsDeleted);

                boolean added = harvestedHeaders.add(receivedHeader);
                if (added)
                    logger.debug("Added OAI header to list: {}", receivedHeader);

            }
        }
    }

    /**
     * @param timestamp the timestamp to parse, {@code null} allowed
     * @return the date parsed from the timestamp or {@code null} if timestamp was {@code null} or empty string
     * @throws IllegalArgumentException if string parameter does not conform to lexical value space defined in XML Schema Part 2: Datatypes for xsd:dateTime.
     */
    private Date parseNullableDateTime(String timestamp) throws IllegalArgumentException {
        Date date;
        if (StringUtils.isBlank(timestamp)) {
            date = null;
        } else {
            date = DatatypeConverter.parseDateTime(timestamp).getTime();
        }
        return date;
    }

}
