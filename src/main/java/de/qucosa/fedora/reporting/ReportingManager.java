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

package de.qucosa.fedora.reporting;

import static org.slf4j.MarkerFactory.getMarker;

import java.net.URI;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import de.qucosa.fedora.mets.MetsHarvester;
import de.qucosa.fedora.oai.OaiHarvester;
import de.qucosa.fedora.oai.OaiHarvesterBuilder;
import de.qucosa.fedora.oai.QucosaDocumentFilter;
import de.qucosa.persistence.PersistenceService;
import de.qucosa.persistence.PostgrePersistenceService;

public class ReportingManager {

    public static final Marker FATAL = getMarker("FATAL");
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private ExecutorService executorService;

    public static void main(String[] args) {
        new ReportingManager().run();
    }

    private void run() {
        try {
            logger.info("Starting up...");
            ReportingProperties prop = ReportingProperties.getInstance();

            
            // initialize OaiHarvester
            //TODO check if PostgrePersistenceService is thread safe, use one service for all components
            PersistenceService persistenceServiceOaiHarvester = new PostgrePersistenceService(
                    prop.getPostgreSQLDatabaseURL(),
                    prop.getPostgreSQLUser(),
                    prop.getPostgreSQLPasswd());

            URI uriToHarvestOAI = new URI(prop.getOaiDataProviderURL());

            //TODO is httpClient closed on shutdown?
            CloseableHttpClient httpClientOaiHarvester = HttpClients.createMinimal();
            
            OaiHarvester oaiHarvester = new OaiHarvesterBuilder(uriToHarvestOAI, httpClientOaiHarvester, persistenceServiceOaiHarvester)
                    .setPollingInterval(Duration.standardSeconds(prop.getOaiDataProviderPollingInterval()))
                    .setOaiHeaderFilter(new QucosaDocumentFilter())
                    .setFC3CompatibilityMode(prop.getFC3CompatibilityMode())
                    .setOaiRunResultHistory(prop.getOaiRunResultHistoryLength())
                    .build();
            
            
            // initialize MetsHarvester
            PersistenceService persistenceServiceMetsHarvester = new PostgrePersistenceService(
                    prop.getPostgreSQLDatabaseURL(),
                    prop.getPostgreSQLUser(),
                    prop.getPostgreSQLPasswd());
            
            URI metsUri = new URI(prop.getMetsDisseminationURL());
            Duration pollInterval = Duration.standardSeconds(prop.getMetsDisseminationPollingInterval());
            Duration minimumWaittimeBetweenTwoRequests = Duration.standardSeconds(1);
            HashMap<String, String> metsXmlPrefixes = prop.getMetsXmlPrefixes();

            //TODO is httpClient closed on shutdown?
            CloseableHttpClient httpClientMetsHarvester = HttpClients.createMinimal();             
             
            MetsHarvester metsHarvester = new MetsHarvester(metsUri, pollInterval,
            minimumWaittimeBetweenTwoRequests, metsXmlPrefixes, persistenceServiceMetsHarvester, httpClientMetsHarvester);

            executorService = Executors.newCachedThreadPool();
            executorService.execute(oaiHarvester);
            executorService.execute(metsHarvester);

            logger.info("Started");

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    logger.info("Shutting down...");
                    executorService.shutdown();
                    if (!executorService.isShutdown()) {
                        logger.info("Still processing. Waiting for orderly shut down...");
                        try {
                            executorService.awaitTermination(1, TimeUnit.MINUTES);
                        } catch (InterruptedException e) {
                            logger.warn("Orderly shut down was interrupted!");
                        }
                    }
                    logger.info("Shut down completed");
                }
            });

        } catch (Exception e) {
            logger.error(FATAL, "OAI harvester was not started!", e);
            System.exit(1);
        }
    }

}
