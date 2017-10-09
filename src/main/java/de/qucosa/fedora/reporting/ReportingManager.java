package de.qucosa.fedora.reporting;

import static org.slf4j.MarkerFactory.getMarker;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import de.qucosa.fedora.mets.MetsProcessor;
import de.qucosa.fedora.oai.OaiHarvester;
import de.qucosa.fedora.oai.OaiHarvesterBuilder;
import de.qucosa.fedora.oai.QucosaDocumentFilter;
import de.qucosa.persistence.PersistenceService;
import de.qucosa.persistence.PostgrePersistenceService;

public class ReportingManager implements ServletContextListener {
    public static final Marker FATAL = getMarker("FATAL");

    private Logger logger = LoggerFactory.getLogger(getClass());

    private Thread thread = null;

    private ServletContext servletContext;

    private ExecutorService executorService;

    @Override
    public void contextInitialized(ServletContextEvent sve) {
        try {
            logger.info("Starting up...");
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
            HashMap<String, String> metsXmlPrefixes = new HashMap<String, String>() {{
                put("mets", "http://www.loc.gov/METS/");
                put("mods", "http://www.loc.gov/mods/v3");
            }};

            //TODO is httpClient closed on shutdown?
            CloseableHttpClient httpClientMetsHarvester = HttpClients.createMinimal();

            MetsProcessor metsHarvester = new MetsProcessor(metsUri, pollInterval,
            minimumWaittimeBetweenTwoRequests, metsXmlPrefixes, persistenceServiceMetsHarvester, httpClientMetsHarvester);

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

                    logger.info("Shut down completed");
                }
            });
        } catch (IOException e) {
            logger.error(FATAL, "OAI harvester was not started!", e);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            logger.error(FATAL, "SQL driver not found!", e);
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sve) {
//        thread.interrupt();
    }

}
