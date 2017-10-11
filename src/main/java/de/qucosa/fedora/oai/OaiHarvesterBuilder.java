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

import de.qucosa.persistence.PersistenceService;

import org.apache.http.impl.client.CloseableHttpClient;
import org.joda.time.Duration;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;

public class OaiHarvesterBuilder {

    // TODO public ok, so everybody can see the defaults?
    public static final boolean DEFAULT_FCREPO3_COMPATIBILITY_MODE = true;
    public static final Duration DEFAULT_POLLING_INTERVAL = Duration.standardMinutes(5);
    public static final Duration MINIMUM_POLLING_INTERVAL = Duration.standardSeconds(1);
    public static final Duration MINIMUM_WAITTIME_BETWEEN_TWO_REQUESTS = Duration.standardSeconds(1);
    public static final Duration DEFAULT_OAI_RUN_RESULT_HISTORY_LENGTH = Duration.standardDays(2);
    public static final OaiHeaderFilter DEFAULT_OAI_HEADER_FILTER = new OaiHeaderFilter() {
        @Override
        public List<OaiHeader> filterOaiHeaders(List<OaiHeader> identifiers) {
            return new LinkedList<>(identifiers);
        }
    };    
    
    private final PersistenceService persistenceService;
    private final URI uriToHarvest;
    private final CloseableHttpClient httpClient;
    
    private OaiHeaderFilter oaiHeaderFilter = DEFAULT_OAI_HEADER_FILTER;
    private Duration oaiRunResultHistory = DEFAULT_OAI_RUN_RESULT_HISTORY_LENGTH;
    private Duration pollingInterval = DEFAULT_POLLING_INTERVAL;
    private boolean useFC3CompatibilityMode = DEFAULT_FCREPO3_COMPATIBILITY_MODE;

    /**
     * @param uriToHarvest the OAI service provider's URI  
     * @param httpClient to be used by {@link OaiHarvester} for communication with the OAI service provider
     * @param persistenceService to be used by {@link OaiHarvester} to persist harvested data and its status
     */
    public OaiHarvesterBuilder(URI uriToHarvest, CloseableHttpClient httpClient, PersistenceService persistenceService) {
        this.uriToHarvest = uriToHarvest;
        this.httpClient = httpClient;
        this.persistenceService = persistenceService;
    }

    public OaiHarvester build() {
        return new OaiHarvester(uriToHarvest, pollingInterval, MINIMUM_WAITTIME_BETWEEN_TWO_REQUESTS, oaiHeaderFilter,
                persistenceService, oaiRunResultHistory, useFC3CompatibilityMode, httpClient);
    }

    /**
     * Sets the specified polling interval. If {@code pollInterval} is shorter
     * than {@link #MINIMUM_POLLING_INTERVAL}, {@link #MINIMUM_POLLING_INTERVAL}
     * is set.
     *
     * @param pollInterval
     * @return this {@link OaiHarvesterBuilder} instance, never {@code null}.
     */
    public OaiHarvesterBuilder setPollingInterval(Duration pollInterval) {
        this.pollingInterval = pollInterval.isShorterThan(OaiHarvesterBuilder.MINIMUM_POLLING_INTERVAL)
                ? OaiHarvesterBuilder.MINIMUM_POLLING_INTERVAL : pollInterval;
        return this;
    }

    /**
     * @param oaiRunResultHistory to set
     * @return this {@link OaiHarvesterBuilder} instance, never {@code null}.
     */
    public OaiHarvesterBuilder setOaiRunResultHistory(Duration oaiRunResultHistory) {
        this.oaiRunResultHistory = oaiRunResultHistory;
        return this;
    }

    /**
     * @param oaiHeaderFilter to set
     * @return this {@link OaiHarvesterBuilder} instance, never {@code null}.
     */
    public OaiHarvesterBuilder setOaiHeaderFilter(OaiHeaderFilter oaiHeaderFilter) {
        this.oaiHeaderFilter = oaiHeaderFilter;
        return this;
    }

    /**
     * Set Fedora Commons 3 compatibility mode to use several workarounds of Fedora Commons 3 bugs such 
     * as broken time stamp format, missing the 'Z' in the end; resumption token flow control bug, not 
     * closing tha last page of a paginated response with an empty resumption token    
     * 
     * @param useFC3CompatibilityMode {@code true} if Fedora Commons 3 compatibility mode has to be used. 
     * @return this {@link OaiHarvesterBuilder} instance, never {@code null}.
     */
    public OaiHarvesterBuilder setFC3CompatibilityMode(boolean useFC3CompatibilityMode) {
        this.useFC3CompatibilityMode = useFC3CompatibilityMode;
        return this;
    }

    public PersistenceService getPersistenceService() {
        return persistenceService;
    }

    public URI getUriToHarvest() {
        return uriToHarvest;
    }

    public CloseableHttpClient getHttpClient() {
        return httpClient;
    }

    public OaiHeaderFilter getOaiHeaderFilter() {
        return oaiHeaderFilter;
    }

    public Duration getOaiRunResultHistory() {
        return oaiRunResultHistory;
    }

    public Duration getPollingInterval() {
        return pollingInterval;
    }

    public boolean isUseFC3CompatibilityMode() {
        return useFC3CompatibilityMode;
    }

    
}
