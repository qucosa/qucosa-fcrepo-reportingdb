package de.qucosa.fedora.oai;

import java.net.URI;

import org.apache.http.impl.client.CloseableHttpClient;
import org.joda.time.Duration;

import de.qucosa.persistence.PersistenceService;

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
        // TODO Auto-generated constructor stub
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
//    @NonNull
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
