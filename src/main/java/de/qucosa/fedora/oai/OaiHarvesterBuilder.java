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

package de.qucosa.fedora.oai;

import de.qucosa.persistence.PersistenceService;
import org.eclipse.jdt.annotation.NonNull;
import org.joda.time.Duration;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;

public class OaiHarvesterBuilder {

    // TODO public ok, so everybody can see the defaults?
    public static final boolean DEFAULT_FCREPO3_COMPATIBILITY_MODE = true;
    public static final Duration DEFAULT_POLLING_INTERVAL = Duration.standardMinutes(5);
    public static final Duration MINIMUM_POLLING_INTERVAL = Duration.standardSeconds(1);
    public static final Duration DEFAULT_OAI_RUN_RESULT_HISTORY_LENGTH = Duration.standardDays(2);
    public static final OaiHeaderFilter DEFAULT_OAI_HEADER_FILTER = new OaiHeaderFilter() {
        @Override
        public List<OaiHeader> filterOaiHeaders(List<OaiHeader> identifiers) {
            return new LinkedList<>(identifiers);
        }
    };
    private final PersistenceService persistenceService;
    private final URI uriToHarvest;
    private OaiHeaderFilter oaiHeaderFilter = DEFAULT_OAI_HEADER_FILTER;
    private Duration oaiRunResultHistory = DEFAULT_OAI_RUN_RESULT_HISTORY_LENGTH;
    private Duration pollingInterval = DEFAULT_POLLING_INTERVAL;
    private boolean useFC3CompatibilityMode = DEFAULT_FCREPO3_COMPATIBILITY_MODE;

    public OaiHarvesterBuilder(URI uriToHarvest, PersistenceService persistenceService) {
        this.uriToHarvest = uriToHarvest;
        this.persistenceService = persistenceService;
    }

    public OaiHarvester build() {
        return new OaiHarvester(uriToHarvest, pollingInterval, oaiHeaderFilter, persistenceService, oaiRunResultHistory,
                useFC3CompatibilityMode);
    }

    /**
     * Sets the specified polling interval. If {@code pollInterval} is shorter
     * than {@link #MINIMUM_POLLING_INTERVAL}, {@link #MINIMUM_POLLING_INTERVAL}
     * is set.
     *
     * @param pollInterval
     * @return
     */
    @NonNull
    public OaiHarvesterBuilder setPollingInterval(Duration pollInterval) {
        this.pollingInterval = pollInterval.isShorterThan(OaiHarvesterBuilder.MINIMUM_POLLING_INTERVAL)
                ? OaiHarvesterBuilder.MINIMUM_POLLING_INTERVAL : pollInterval;
        return this;
    }

    @NonNull
    public OaiHarvesterBuilder setOaiRunResultHistory(Duration oaiRunResultHistory) {
        this.oaiRunResultHistory = oaiRunResultHistory;
        return this;
    }

    @NonNull
    public OaiHarvesterBuilder setOaiHeaderFilter(OaiHeaderFilter oaiHeaderFilter) {
        this.oaiHeaderFilter = oaiHeaderFilter;
        return this;
    }

    @NonNull
    public OaiHarvesterBuilder setFC3CompatibilityMode(boolean useFC3CompatibilityMode) {
        this.useFC3CompatibilityMode = useFC3CompatibilityMode;
        return this;
    }

}
