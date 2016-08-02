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

package de.slub.fedora.reporting;

import java.net.URI;
import java.net.URISyntaxException;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;

import de.slub.fedora.oai.OaiHarvester;
import de.slub.fedora.oai.OaiHarvesterBuilder;
import de.slub.fedora.oai.QucosaDocumentFilter;
import de.slub.persistence.PersistenceService;
import de.slub.persistence.PostgrePersistenceService;

public class ReportingManager {

	private final Logger logger = LoggerFactory.getLogger(getClass());


	private void init() {

		ReportingProperties prop = ReportingProperties.getInstance();

		PersistenceService persistenceService = new PostgrePersistenceService(prop.getPostgreSQLDatabaseURL(),
				prop.getPostgreSQLUser(), prop.getPostgreSQLPasswd());

		try {

			OaiHarvester oaiHarvester = new OaiHarvesterBuilder(
					new URI(prop.getOaiDataProviderURL()), persistenceService)
					.setPollingInterval(Duration.standardSeconds(prop.getOaiDataProviderPollingInterval()))
					.setOaiHeaderFilter(new QucosaDocumentFilter())
					.setFC3CompatibilityMode(prop.getFC3CompatibilityMode())
					.setOaiRunResultHistory(prop.getOaiRunResultHistoryLength())
					.build();

			Thread thread = new Thread(oaiHarvester);
			thread.start();

		} catch (URISyntaxException e) {
			logger.error(MarkerFactory.getMarker("FATAL"), "OAI harvester was not started. Exception: " + e);
			System.exit(1);
		}

	}

	public static void main(String[] args) {

		new ReportingManager().init();
	}

}
