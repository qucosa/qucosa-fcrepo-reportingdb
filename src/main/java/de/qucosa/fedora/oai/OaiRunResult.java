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

import org.apache.commons.lang3.StringUtils;

import java.util.Date;

/**
 * The harvester's status of a successful request.
 * <p>
 * An immutable OaiRunResult always has a time stamp when this run was executed
 * and the servers responseDate. Optional parameters are OAI resumption token
 * and the resumption token's expiration date.
 */
public final class OaiRunResult {

    /**
     * The value of the 'from' parameter to be used in the next GET request not
     * containing a resumptionToken. To be used to start from the first page in 
     * case a paginated response with resumption token expired.
     */
    private final Date nextFromTimestamp;
    /**
     * time stamp received from OAI data provider that was requested
     */
    private final Date responseDate;

    /**
     * optional resumptionToken as contained in response
     */
    private final String resumptionToken;

    /**
     * the OAI resumption token's expiration date
     */
    private final Date resumptionTokenExpirationDate;
    /**
     * local time stamp when this run was started
     */
    private final Date timestampOfRun;

    /**
     * @param timestampOfRun                time stamp when this run was executed
     * @param responseDate                  time stamp received from OAI data provider that was requested.
     * @param resumptionToken               the OAI resumption token received. either {@code null} if
     *                                      there is no resumptionToken, or empty String "" if there
     *                                      is an empty resumptionToken or a String with length() > 0
     *                                      containing the resumptionTokens's value.<br />
     *                                      may be {@code null} iff resumptionTokenExpirationDate is
     *                                      {@code null}.
     * @param resumptionTokenExpirationDate the OAI resumption token's expiration date, may be
     *                                      {@code null}.
     * @param nextFromTimestamp             The timestamp of the 'from' parameter to be used in the next
     *                                      GET request not containing a resumptionToken. To be used in
     *                                      case a paginated response with resumption token expires to
     *                                      start from the first page. May be {@code null}.
     * @throws IllegalArgumentException 1) if timestampOfRun is {@code null} or <br />
     *                                  2) if responseDate is {@code null} or <br />
     *                                  3) if resumptionTokenExpirationDate is not {@code null} but
     *                                  resumptionToken is whitespace, empty ("") or null.
     */
    public OaiRunResult(Date timestampOfRun, Date responseDate, String resumptionToken,
                        Date resumptionTokenExpirationDate, Date nextFromTimestamp)
            throws IllegalArgumentException {

        if (timestampOfRun == null)
            throw new IllegalArgumentException(
                    "Parameter timestampOfRun must not be null. Use OaiRunResult() to construct a dummy object with null values.");

        if (responseDate == null)
            throw new IllegalArgumentException(
                    "Parameter responseDate must not be null. Use OaiRunResult() to construct a dummy object. with null values");

        if (StringUtils.isBlank(resumptionToken) && resumptionTokenExpirationDate != null) {
            throw new IllegalArgumentException("resumptionTokenExpirationDate is not allowed "
                    + "if parameter resumptionToken is whitespace, empty string or null .");
        }

        this.timestampOfRun = timestampOfRun;
        this.responseDate = responseDate;
        this.resumptionToken = resumptionToken;
        this.resumptionTokenExpirationDate = resumptionTokenExpirationDate;
        this.nextFromTimestamp = nextFromTimestamp;
    }

    //TODO do we really need this empty OaiRunResult? It forces all getters to possibly return null 
    // hence every caller has to do null checks :/ 
    public OaiRunResult() {
        this.timestampOfRun = null;
        this.responseDate = null;
        this.resumptionToken = null;
        this.resumptionTokenExpirationDate = null;
        this.nextFromTimestamp = null;
    }

    /**
     * @return local time stamp when this run was started. May be {@code null}.
     */
//    @Nullable
    public Date getTimestampOfRun() {
        return timestampOfRun;
    }

    /** 
     * @return  time stamp received from OAI data provider that was requested. May be {@code null}.
     */
//    @Nullable
    public Date getResponseDate() {
        return responseDate;
    }

    /**
     * @return either {@code null} if there is no resumptionToken, or empty
     * String "" tr represent an empty resumptionToken or a String with
     * length() > 0 containing the resumptionTokens's value.
     */
//    @Nullable
    public String getResumptionToken() {
        return resumptionToken;
    }

    
    
    /**
     * @return the OAI resumption token's expiration date. May be {@code null}.
     */
//    @Nullable
    public Date getResumptionTokenExpirationDate() {
        return resumptionTokenExpirationDate;
    }

    /**
     * The value of the 'from' parameter to be used in the next GET request not
     * containing a resumptionToken. To be used to start from the first page in 
     * case a paginated response with resumption token expired.
     * 
     * @return May be {@code null}.
     */
//    @Nullable 
    public Date getNextFromTimestamp() {
        return nextFromTimestamp;
    }

    public boolean hasTimestampOfRun() {
        return timestampOfRun != null;
    }

    public boolean hasNextFromTimestamp() {
        return nextFromTimestamp != null;
    }

    public boolean hasResumptionToken() {
        return resumptionToken != null && !resumptionToken.isEmpty();
    }

    // deactivated to prevent false negatives, caused by asynchronous local and
    // remote clocks
    // public boolean isValidResumptionToken(Date now) {
    // return resumptionToken != null && !resumptionToken.isEmpty()
    // && (resumptionTokenExpirationDate == null ||
    // resumptionTokenExpirationDate.after(now));
    // }

    @Override
    public String toString() {
        return "OaiRunResult [timestampOfRun=" + timestampOfRun + ", responseDate=" + responseDate
                + ", resumptionToken=" + resumptionToken + ", resumptionTokenExpirationDate="
                + resumptionTokenExpirationDate + ", nextFromTimestamp=" + nextFromTimestamp + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((nextFromTimestamp == null) ? 0 : nextFromTimestamp.hashCode());
        result = prime * result + ((responseDate == null) ? 0 : responseDate.hashCode());
        result = prime * result + ((resumptionToken == null) ? 0 : resumptionToken.hashCode());
        result = prime * result
                + ((resumptionTokenExpirationDate == null) ? 0 : resumptionTokenExpirationDate.hashCode());
        result = prime * result + ((timestampOfRun == null) ? 0 : timestampOfRun.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        OaiRunResult other = (OaiRunResult) obj;
        if (nextFromTimestamp == null) {
            if (other.nextFromTimestamp != null)
                return false;
        } else if (!nextFromTimestamp.equals(other.nextFromTimestamp))
            return false;
        if (responseDate == null) {
            if (other.responseDate != null)
                return false;
        } else if (!responseDate.equals(other.responseDate))
            return false;
        if (resumptionToken == null) {
            if (other.resumptionToken != null)
                return false;
        } else if (!resumptionToken.equals(other.resumptionToken))
            return false;
        if (resumptionTokenExpirationDate == null) {
            if (other.resumptionTokenExpirationDate != null)
                return false;
        } else if (!resumptionTokenExpirationDate.equals(other.resumptionTokenExpirationDate))
            return false;
        if (timestampOfRun == null) {
            if (other.timestampOfRun != null)
                return false;
        } else if (!timestampOfRun.equals(other.timestampOfRun))
            return false;
        return true;
    }

}
