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

package de.qucosa.fedora.mets;

import java.util.Date;

import org.apache.commons.lang3.StringUtils;

public class ReportingDocumentMetadata {

    // @NonNull
    private final String recordIdentifier;

    // @NonNull
    private final String mandator;

    // @NonNull
    private final String documentType;

    // @NonNull
    private final Date distributionDate;

    // @NonNull
    private final Date headerLastModified;

    
    
    /**
     * @param recordIdentifier   the unique identifier of this item in a repository
     * @param mandator           the mandator this item is related to
     * @param documentType       a document type such as article, book, issue
     * @param distributionDate   the date this item has been published in the repository 
     * @param headerLastModified the date this item's metadata has been updated in the repository
     * @throws IllegalArgumentException if recordIdentifier, mandator or documentType is whitespace, empty ("")
     *                           or {@code null} or if distributionDate or headerLastModified is {@code null}
     */
    public ReportingDocumentMetadata(String recordIdentifier, String mandator, String documentType,
                                    Date distributionDate, Date headerLastModified)  throws IllegalArgumentException {

        if (StringUtils.isBlank(recordIdentifier))
            throw new IllegalArgumentException("Parameter recordIdentifier must not be '" + recordIdentifier + "'.");
        if (StringUtils.isBlank(mandator))
            throw new IllegalArgumentException("Parameter mandator must not be '" + mandator + "'.");
        if (StringUtils.isBlank(documentType))
            throw new IllegalArgumentException("Parameter documentType must not be '" + documentType + "'.");
        if (distributionDate == null)
            throw new IllegalArgumentException("Parameter distributionDate must not be null.");
        if (headerLastModified == null)
            throw new IllegalArgumentException("Parameter headerLastModified must not be null.");

        this.recordIdentifier = recordIdentifier;
        this.mandator = mandator;
        this.documentType = documentType;
        this.distributionDate = distributionDate;
        this.headerLastModified = headerLastModified;
    }

    /**
     * @return the unique identifier of this item in a repository, never {@code null}.
     */
    public String getRecordIdentifier() {
        return recordIdentifier;
    }

    /**
     * @return the mandator this item is related to, never {@code null}.
     */
    public String getMandator() {
        return mandator;
    }

    /**
     * @return a document type such as article, book, issue, never {@code null}.
     */
    public String getDocumentType() {
        return documentType;
    }

    /**
     * @return the date this item has been published in the repository, never {@code null}.
     */
    public Date getDistributionDate() {
        return distributionDate;
    }

    /**
     * @return the date this item's metadata has been updated in the repository, never {@code null}.
     */
    public Date getHeaderLastModified() {
        return headerLastModified;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((distributionDate == null) ? 0 : distributionDate.hashCode());
        result = prime * result + ((documentType == null) ? 0 : documentType.hashCode());
        result = prime * result + ((headerLastModified == null) ? 0 : headerLastModified.hashCode());
        result = prime * result + ((mandator == null) ? 0 : mandator.hashCode());
        result = prime * result + ((recordIdentifier == null) ? 0 : recordIdentifier.hashCode());
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
        ReportingDocumentMetadata other = (ReportingDocumentMetadata) obj;
        if (distributionDate == null) {
            if (other.distributionDate != null)
                return false;
        } else if (!distributionDate.equals(other.distributionDate))
            return false;
        if (documentType == null) {
            if (other.documentType != null)
                return false;
        } else if (!documentType.equals(other.documentType))
            return false;
        if (headerLastModified == null) {
            if (other.headerLastModified != null)
                return false;
        } else if (!headerLastModified.equals(other.headerLastModified))
            return false;
        if (mandator == null) {
            if (other.mandator != null)
                return false;
        } else if (!mandator.equals(other.mandator))
            return false;
        if (recordIdentifier == null) {
            if (other.recordIdentifier != null)
                return false;
        } else if (!recordIdentifier.equals(other.recordIdentifier))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "ReportingDocumentMetadata [recordIdentifier=" + recordIdentifier + ", mandator=" + mandator
                + ", documentType=" + documentType + ", distributionDate=" + distributionDate + ", headerLastModified="
                + headerLastModified + "]";
    }

    
    
}
