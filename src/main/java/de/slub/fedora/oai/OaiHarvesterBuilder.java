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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.eclipse.jdt.annotation.NonNull;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.slf4j.Logger;

import de.slub.persistence.PersistenceService;

public class OaiHarvesterBuilder {

	public static final String DEFAULT_URL = "http://localhost:8080/fedora/oai";
	public static final TimeValue DEFAULT_POLLING_INTERVAL = new TimeValue(5, TimeUnit.MINUTES);
	private static final TimeValue DEFAULT_OAI_RUN_RESULT_HISTORY_MILLIS = new TimeValue(2, TimeUnit.DAYS);
	private static final OaiHeaderFilter DEFAULT_OAI_HEADER_FILTER = new OaiHeaderFilter() {
		@Override
		public List<OaiHeader> filterOaiHeaders(List<OaiHeader> identifiers) {
			return new LinkedList<>(identifiers);
		}
	};

	private Logger logger;
	private URL url;
	private PersistenceService persistenceService;
	private TimeValue pollingInterval;
	private TimeValue oaiRunResultHistory;
	private OaiHeaderFilter oaiHeaderFilter;


	public OaiHarvester build() throws MalformedURLException, URISyntaxException {
		return new OaiHarvester((url == null) ? new URL(DEFAULT_URL) : url,
				(pollingInterval == null) ? DEFAULT_POLLING_INTERVAL : pollingInterval, 
				persistenceService,
				(oaiRunResultHistory == null) ? DEFAULT_OAI_RUN_RESULT_HISTORY_MILLIS : oaiRunResultHistory,
				(oaiHeaderFilter == null) ? DEFAULT_OAI_HEADER_FILTER : oaiHeaderFilter, logger);
	}

	public OaiHarvesterBuilder settings(Map<String, Object> oaiSettings) throws MalformedURLException {

		if (oaiSettings.containsKey("poll_interval")) {
			pollingInterval = TimeValue.parseTimeValue(String.valueOf(oaiSettings.get("poll_interval")),
					DEFAULT_POLLING_INTERVAL);
		}
		if (oaiSettings.containsKey("url")) {
			url = new URL(XContentMapValues.nodeStringValue(oaiSettings.get("url"), DEFAULT_URL));
		}
		return this;
	}


	// TODO: why do we want to set a logger?
	@NonNull
	public OaiHarvesterBuilder setLogger(@NonNull Logger logger) {
		this.logger = logger;
		return this;
	}

	@NonNull
	public OaiHarvesterBuilder setUrl(@NonNull URL url) {
		this.url = url;
		return this;
	}

	@NonNull
	public OaiHarvesterBuilder setPollingInterval(@NonNull TimeValue interval) {
		this.pollingInterval = interval;
		return this;
	}

	@NonNull
	public OaiHarvesterBuilder setOaiRunResultHistory(@NonNull TimeValue oaiRunResultHistory) {
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
