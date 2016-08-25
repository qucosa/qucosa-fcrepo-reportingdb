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

import de.qucosa.fedora.oai.OaiHarvester;
import de.qucosa.fedora.oai.OaiHarvesterBuilder;
import de.qucosa.fedora.oai.QucosaDocumentFilter;
import de.qucosa.persistence.PersistenceService;
import de.qucosa.persistence.PostgrePersistenceService;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.net.URI;

import static org.slf4j.MarkerFactory.getMarker;

public class ReportingManager {

    public static final Marker FATAL = getMarker("FATAL");
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) {
        new ReportingManager().init();
    }

    private void init() {
        try {
            ReportingProperties prop = ReportingProperties.getInstance();

            PersistenceService persistenceService = new PostgrePersistenceService(
                    prop.getPostgreSQLDatabaseURL(),
                    prop.getPostgreSQLUser(),
                    prop.getPostgreSQLPasswd());

            URI uriToHarvest = new URI(prop.getOaiDataProviderURL());

            OaiHarvester oaiHarvester = new OaiHarvesterBuilder(uriToHarvest, persistenceService)
                    .setPollingInterval(Duration.standardSeconds(prop.getOaiDataProviderPollingInterval()))
                    .setOaiHeaderFilter(new QucosaDocumentFilter())
                    .setFC3CompatibilityMode(prop.getFC3CompatibilityMode())
                    .setOaiRunResultHistory(prop.getOaiRunResultHistoryLength())
                    .build();

            Thread thread = new Thread(oaiHarvester);
            thread.start();

        } catch (Exception e) {
            logger.error(FATAL, "OAI harvester was not started: {}", e);
            System.exit(1);
        }
    }

}
