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

/**
 * Helper for JUnit tests to set aggressive timings that should not be used in production environment.
 * 
 * 
 * @author reichert
 *
 */
public class OaiHarvesterBuilderHelper extends OaiHarvesterBuilder {

    private Duration pollingIntervalUnitTest = DEFAULT_POLLING_INTERVAL;
    private Duration minimumWaittimeBetweenTwoRequests = MINIMUM_WAITTIME_BETWEEN_TWO_REQUESTS;
    
    protected OaiHarvesterBuilderHelper(URI uriToHarvest, CloseableHttpClient httpClient,
            PersistenceService persistenceService) {
        super(uriToHarvest, httpClient, persistenceService);
    }
    
    public OaiHarvester build() {
        return new OaiHarvester(getUriToHarvest(), pollingIntervalUnitTest, minimumWaittimeBetweenTwoRequests,  
                getOaiHeaderFilter(), getPersistenceService(), getOaiRunResultHistory(), isUseFC3CompatibilityMode(),
                getHttpClient());
    }
    
    /**
     * Sets the specified polling interval. In contrast to 
     * {@link OaiHarvesterBuilder#setPollingInterval(Duration)}, this method does not enforce a minimum
     * value so any value is accepted.
     *
     * @param pollInterval
     * @return this {@link OaiHarvesterBuilderHelper} instance, never {@code null}.
     */
    protected OaiHarvesterBuilderHelper setPollingIntervalForUnitTest(Duration pollIntervalForUnitTest) {
        this.pollingIntervalUnitTest = pollIntervalForUnitTest;
        return this;
    }
    
    /**
     * Set the interval between two paginated requests (response with resumption token).  
     * 
     * @param minimumWaittimeBetweenTwoRequests
     * @return
     */
    protected OaiHarvesterBuilderHelper setMinimumWaittimeBetweenTwoRequests(Duration minimumWaittimeBetweenTwoRequests){
        this.minimumWaittimeBetweenTwoRequests = minimumWaittimeBetweenTwoRequests;
        return this;
    }

}
