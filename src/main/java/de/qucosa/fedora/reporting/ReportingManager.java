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

package de.qucosa.fedora.reporting;

import de.qucosa.fedora.mets.MetsProcessor;
import de.qucosa.fedora.oai.OaiHarvester;
import de.qucosa.fedora.oai.OaiHarvesterBuilder;
import de.qucosa.fedora.oai.QucosaDocumentFilter;
import de.qucosa.persistence.PersistenceService;
import de.qucosa.persistence.PostgrePersistenceService;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.slf4j.MarkerFactory.getMarker;

public class ReportingManager implements ServletContextListener {

    public static final Marker FATAL = getMarker("FATAL");
    private ExecutorService executorService;
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void contextInitialized(ServletContextEvent sve) {
        logger.info("Starting up...");
        try {
            ReportingProperties prop = ReportingProperties.getInstance();

            // initialize OaiHarvester
            //TODO check if PostgrePersistenceService is thread safe, use one service for all components
            PersistenceService persistenceServiceOaiHarvester = new PostgrePersistenceService(
                    prop.getPostgreSQLDriver(),
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
                    prop.getPostgreSQLDriver(),
                    prop.getPostgreSQLDatabaseURL(),
                    prop.getPostgreSQLUser(),
                    prop.getPostgreSQLPasswd());

            URI metsUri = new URI(prop.getMetsDisseminationURL());
            Duration pollInterval = Duration.standardSeconds(prop.getMetsDisseminationPollingInterval());
            Duration minimumWaittimeBetweenTwoRequests = Duration.standardSeconds(1);

            //TODO is httpClient closed on shutdown?
            CloseableHttpClient httpClientMetsHarvester = HttpClients.createMinimal();

            MetsProcessor metsHarvester = new MetsProcessor(metsUri, pollInterval,
                    minimumWaittimeBetweenTwoRequests, persistenceServiceMetsHarvester, httpClientMetsHarvester);

            executorService = Executors.newCachedThreadPool();
            executorService.execute(oaiHarvester);
            executorService.execute(metsHarvester);

            logger.info("Started");

            Runtime.getRuntime().addShutdownHook(new Thread() {

                @Override
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
                }
            });

        } catch (IOException e) {
            logger.error(FATAL, "OAI harvester was not started!", e);
        } catch (IllegalArgumentException | URISyntaxException e) {
            logger.error(FATAL, e.getMessage(), e);
        } catch (SQLException e) {
            logger.error(FATAL, "SQL driver not found!", e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        logger.info("Shut down completed");
    }

}
