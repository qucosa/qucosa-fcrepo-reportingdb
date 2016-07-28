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

package de.slub.fedora.oai;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

import org.eclipse.jdt.annotation.NonNull;
import org.joda.time.Duration;

import de.slub.persistence.PersistenceService;

public class OaiHarvesterBuilder {

	public static final String DEFAULT_URL = "http://localhost:8080/fedora/oai";
	public static final Duration DEFAULT_POLLING_INTERVAL = Duration.standardMinutes(5);
	private static final Duration DEFAULT_OAI_RUN_RESULT_HISTORY = Duration.standardDays(2);
	private static final OaiHeaderFilter DEFAULT_OAI_HEADER_FILTER = new OaiHeaderFilter() {
		@Override
		public List<OaiHeader> filterOaiHeaders(List<OaiHeader> identifiers) {
			return new LinkedList<>(identifiers);
		}
	};

	private URL url;
	private PersistenceService persistenceService;
	private Duration pollingInterval;
	private Duration oaiRunResultHistory;
	private OaiHeaderFilter oaiHeaderFilter;


	public OaiHarvester build() throws MalformedURLException, URISyntaxException {
		return new OaiHarvester((url == null) ? new URL(DEFAULT_URL) : url,
				(pollingInterval == null) ? DEFAULT_POLLING_INTERVAL : pollingInterval, 
				persistenceService,
				(oaiRunResultHistory == null) ? DEFAULT_OAI_RUN_RESULT_HISTORY : oaiRunResultHistory,
				(oaiHeaderFilter == null) ? DEFAULT_OAI_HEADER_FILTER : oaiHeaderFilter);
	}


	@NonNull
	public OaiHarvesterBuilder setUrl(@NonNull URL url) {
		this.url = url;
		return this;
	}

	@NonNull
	public OaiHarvesterBuilder setPollingInterval(@NonNull Duration interval) {
		this.pollingInterval = interval;
		return this;
	}

	@NonNull
	public OaiHarvesterBuilder setOaiRunResultHistory(@NonNull Duration oaiRunResultHistory) {
		this.oaiRunResultHistory = oaiRunResultHistory;
		return this;
	}

	@NonNull
	public OaiHarvesterBuilder setPersistenceService(@NonNull PersistenceService persistenceService) {
		this.persistenceService = persistenceService;
		return this;
	}

	public OaiHarvesterBuilder setOaiHeaderFilter(OaiHeaderFilter oaiHeaderFilter) {
		this.oaiHeaderFilter = oaiHeaderFilter;
		return this;
	}

}
