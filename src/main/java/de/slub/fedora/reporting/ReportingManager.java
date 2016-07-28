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

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.slub.fedora.oai.OaiHarvester;
import de.slub.fedora.oai.OaiHarvesterBuilder;
import de.slub.fedora.oai.QucosaDocumentFilter;
import de.slub.persistence.PersistenceService;
import de.slub.persistence.PostgrePersistenceService;
import de.slub.persistence.ReportingProperties;

public class ReportingManager {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	//FIXME: load from properties
	private static final String URL_OAI_SERVER_TEST = "http://sdvcmr-app01:8080/fedora/oai";
	private static final String URL_OAI_SERVER_PRODUCTION = "http://sdvcmr-prod-core01:8080/fedora/oai";
	
	private void init() {

		// FIXME add init logic here
		logger.warn("Implementation of init logic not yet done");
		
		ReportingProperties prop = ReportingProperties.getInstance();
		
		PersistenceService persistenceService = new PostgrePersistenceService(prop.getPostgreSQLDatabaseURL(),
				prop.getPostgreSQLUser(), prop.getPostgreSQLPasswd());
		
		try {
			OaiHarvester oaiHarvester = new OaiHarvesterBuilder().setUrl(new URL(URL_OAI_SERVER_TEST))
					.setPollingInterval(new TimeValue(15, TimeUnit.SECONDS)).setPersistenceService(persistenceService)
					.setLogger(LoggerFactory.getLogger(OaiHarvester.class))
					.setOaiHeaderFilter(new QucosaDocumentFilter()).build();
			Thread thread = new Thread(oaiHarvester);
			thread.start();
		} catch (MalformedURLException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	
	public static void main(String[] args) {

		new ReportingManager().init();
	}

}
