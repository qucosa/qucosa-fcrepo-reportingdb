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

import de.qucosa.fedora.oai.OaiHeader;
import de.qucosa.persistence.PersistenceException;
import de.qucosa.persistence.PersistenceService;
import de.qucosa.util.TerminateableRunnable;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.ws.rs.core.UriBuilder;
import javax.xml.namespace.NamespaceContext;
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
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The {@link MetsProcessor} reads {@link OaiHeader}s from
 * {@link PersistenceService}, requests a external METS disemmination service,
 * processes the XML result, i.e. extracts data relevant for reporting and
 * stores the results in {@link PersistenceService}.
 */
public class MetsProcessor extends TerminateableRunnable {

    public static final String ERROR_MSG_EMPTY_RESPONSE_FROM_METS_DISSEMINATION_SERVICE = "Got empty response from METS dissemination service.";
    public static final String ERROR_MSG_UNEXPECTED_HTTP_RESPONSE = "Unexpected METS dissemination service response HTTP";
    private static final String XPATH_DISTRIBUTION_DATE = "//mods:originInfo[@eventType='distribution']/mods:dateIssued";
    private static final String XPATH_DOCUMENT_TYPE = "//mets:structMap[@TYPE='LOGICAL']/mets:div/@TYPE";
    private static final String XPATH_MANDATOR = "//mets:metsHdr/mets:agent[@ROLE='EDITOR']/mets:name";

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final CloseableHttpClient httpClient;
    private final URI uri;

    /**
     * Interval to poll persistenceService for new OAIHeaders
     */
    private final Duration pollInterval;
    private final Duration minimumWaittimeBetweenTwoRequests;
    private final PersistenceService persistenceService;
    private final SimpleNamespaceContext namespaces;

    // TODO better name.
    private boolean moreOAIHeadersToProcess = true;

    public MetsProcessor(URI harvestingUri, Duration pollInterval, Duration minimumWaittimeBetweenTwoRequests,
            HashMap<String, String> xmlPrefixes, PersistenceService persistenceService,
            CloseableHttpClient httpClient) {
        this.uri = harvestingUri;
        this.pollInterval = pollInterval;
        this.minimumWaittimeBetweenTwoRequests = minimumWaittimeBetweenTwoRequests;
        this.persistenceService = persistenceService;
        this.httpClient = httpClient;
        this.namespaces = new SimpleNamespaceContext(xmlPrefixes);

    }

    @Override
    public void run() {

        logger.info("Requesting METS data from URL: {}", this.uri.toASCIIString());
        do {

            // get OaiHeaders from persistence
            List<OaiHeader> oaiHeadersToProcess = new LinkedList<>();
            try {
                oaiHeadersToProcess = persistenceService.getOaiHeaders();
                if (!oaiHeadersToProcess.isEmpty()) {
                    moreOAIHeadersToProcess = true;
                } else {
                    moreOAIHeadersToProcess = false;
                    continue; // nothing to do, go to sleep
                }
            } catch (PersistenceException e) {
                logger.error("Could not load OaiHeaders from persistence service: ", e);
                moreOAIHeadersToProcess = false;
                continue; // nothing to do, go to sleep
            }

            // request METS dissemination
            List<ReportingDocumentMetadata> reportingDocuments = new LinkedList<>();
            List<OaiHeader> oaiHeadersProcessed = new LinkedList<>();
            for (OaiHeader header : oaiHeadersToProcess) {
                ReportingDocumentMetadata reportingDoc = harvest(header);
                if (reportingDoc != null) {
                    reportingDocuments.add(reportingDoc);
                }
                oaiHeadersProcessed.add(header);

                // wait between 2 requests. If interrupted, do not process
                // the remaining oaiHeadersToProcess (but persist the
                // documents processed so far.)
                if (!waitForNextRun()) {
                    break;
                }

            }

            // store results in persistence
            try {
                persistenceService.addOrUpdateReportingDocuments(reportingDocuments);
                persistenceService.removeOaiHeadersIfUnmodified(oaiHeadersProcessed);
            } catch (PersistenceException e) {
                // TODO @Ralf: what should we do here? Different Messages
                // for both persistence operations?
                // it may happen that we successfully store all
                // ReportingDocumentHeaders but something goes wrong when
                // removing OaiHeaders. Putting both in one transaction
                // doesn't seem required since the persistence is
                // consistent even if we process all OaiHeaders again.
                logger.error("Could not persist ReportingDocumentHeaders: ", e);
            }

            // sleep 
            waitForNextRun();

        } while (isRunning());
    }

    private ReportingDocumentMetadata harvest(OaiHeader header) {
        URI uri = buildMetsRequestURI(header.getRecordIdentifier());
        HttpGet httpGet = new HttpGet(uri);
        ReportingDocumentMetadata reportingDocument = null;
        String errorMsgWithRecordIdentifier = "METS document for id '" + header.getRecordIdentifier() + "' could not be processed.";

        try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
            if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                HttpEntity httpEntity = httpResponse.getEntity();
                if (httpEntity != null) {
                    reportingDocument = handleXmlResult(httpEntity.getContent(), header);
                } else {
                    logger.error("{} {}", errorMsgWithRecordIdentifier, ERROR_MSG_EMPTY_RESPONSE_FROM_METS_DISSEMINATION_SERVICE);
                }
            } else {
                logger.error("{} {} {} {}" , errorMsgWithRecordIdentifier, ERROR_MSG_UNEXPECTED_HTTP_RESPONSE,
                        httpResponse.getStatusLine().getStatusCode(), httpResponse.getStatusLine().getReasonPhrase());
            }
        } catch (Exception ex) {
            logger.error(errorMsgWithRecordIdentifier + ensureMessage(ex));
        }
        return reportingDocument;
    }

    /**
     * @param content
     * @param header
     * @return the {@link ReportingDocumentMetadata} parsed from METS XML or
     *         {@code null} if any error occurred (e.g.
     */
    private ReportingDocumentMetadata handleXmlResult(InputStream content, OaiHeader header) {

        ReportingDocumentMetadata reportingDoc = null;
        try {
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            documentBuilderFactory.setNamespaceAware(true);
            Document document = documentBuilderFactory.newDocumentBuilder().parse(content);

            // TODO nice-to-have: validate httpEntity.getContent() against schema - is it valid mets?

            String documentType = extractDocumentType(document);
            Date distributionDate = extractDistributionDate(document);
            String mandator = extractMandator(document);

            reportingDoc = new ReportingDocumentMetadata(header.getRecordIdentifier(), mandator, documentType,
                    distributionDate, header.getDatestamp());

        } catch (SAXException | IOException | ParserConfigurationException | XPathExpressionException
                | IllegalArgumentException ex) {
            logger.error("METS document for id '{}' could not be parsed or contains incomplete data: {}",
                    getLocalIdentifier(header.getRecordIdentifier()), ensureMessage(ex));
        }

        return reportingDoc;
    }

    private Date extractDistributionDate(Document document) throws XPathExpressionException {

        XPath xPath = XPathFactory.newInstance().newXPath();
        xPath.setNamespaceContext(namespaces);

        XPathExpression xSelectDistributionDate = xPath.compile(XPATH_DISTRIBUTION_DATE);
        String distributionDateString = (String) xSelectDistributionDate.evaluate(document, XPathConstants.STRING);
        Date distributionDate = new Date(new DateTime(distributionDateString).getMillis());

        return distributionDate;
    }

    private String extractDocumentType(Document document) throws XPathExpressionException {

        XPath xPath = XPathFactory.newInstance().newXPath();
        xPath.setNamespaceContext(namespaces);

        XPathExpression xSelectQucosaDocumentType = xPath.compile(XPATH_DOCUMENT_TYPE);
        String qucosaDocumentType = (String) xSelectQucosaDocumentType.evaluate(document, XPathConstants.STRING);

        return qucosaDocumentType;
    }

    private String extractMandator(Document document) throws XPathExpressionException {
        XPath xPath = XPathFactory.newInstance().newXPath();
        xPath.setNamespaceContext(namespaces);

        XPathExpression xSelectMandator = xPath.compile(XPATH_MANDATOR);
        String mandator = (String) xSelectMandator.evaluate(document, XPathConstants.STRING);
        return mandator;
    }

    /**
     * Sleep for {@link #pollInterval} if {@link #moreOAIHeadersToProcess} is
     * {@code false}.
     * 
     * @return {@code false} iff interrupted while waiting, or {@code true} in
     *         any other case.
     */
    private boolean waitForNextRun() {
        long waitTime = 1000l;
        if(moreOAIHeadersToProcess) {
            waitTime = minimumWaittimeBetweenTwoRequests.getMillis();
        } else {
            waitTime = pollInterval.getMillis();
            //TODO would this be useful to see in log?
            logger.info("Nothing to be done. Going to sleep for {} millis", waitTime);
        }

        try {
            TimeUnit.MILLISECONDS.sleep(waitTime);
            return true;
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for next METS run: {}", e.getMessage());
            return false;
        }
    }

    private URI buildMetsRequestURI(String oaiId) {

        UriBuilder builder = UriBuilder.fromUri(uri).queryParam("pid", getLocalIdentifier(oaiId));
        return builder.build();
    }

    private String getLocalIdentifier(String oaiId) {
        return oaiId.substring(oaiId.indexOf(':', "oai:".length()) + 1);
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

    /**
     * taken from
     * http://stackoverflow.com/questions/6390339/how-to-query-xml-using-namespaces-in-java-with-xpath
     *
     */
    class SimpleNamespaceContext implements NamespaceContext {

        private final Map<String, String> PREF_MAP = new HashMap<String, String>();

        public SimpleNamespaceContext(final Map<String, String> prefMap) {
            PREF_MAP.putAll(prefMap);
        }

        public String getNamespaceURI(String prefix) {
            return PREF_MAP.get(prefix);
        }

        public String getPrefix(String uri) {
            throw new UnsupportedOperationException();
        }

        @SuppressWarnings("rawtypes")
        public Iterator getPrefixes(String uri) {
            throw new UnsupportedOperationException();
        }

    }

}
