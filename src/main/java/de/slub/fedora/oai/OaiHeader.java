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

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.jdt.annotation.NonNull;

/**
 * The class represents the header of an OAI Record except for the setSpec
 * elements http://www.openarchives.org/OAI/openarchivesprotocol.html#Record
 */
public class OaiHeader {

    @NonNull
    private final String recordIdentifier;
    @NonNull
    private final Date datestamp;
    @NonNull
    private final List<String> setSpec;
    private final boolean statusIsDeleted;

    /**
     * @param recordIdentifier
     *            the unique identifier of an item in a repository
     * @param datestamp
     *            the date of creation, modification or deletion of the record
     * @param a
     *            list with zero or more elements, each representing the content
     *            of a setSpec element
     * @param statusIsDeleted
     *            true if the header contains the status deleted element
     * @throws IllegalArgumentException
     *             if recordIdentifier or datestamp are whitespace, empty ("")
     *             or null or if setSpec is null
     */
    public OaiHeader(@NonNull String recordIdentifier, @NonNull Date datestamp, @NonNull List<String> setSpec,
            boolean statusIsDeleted) throws IllegalArgumentException {

        if (StringUtils.isBlank(recordIdentifier))
            throw new IllegalArgumentException("parameter recordIdentifier must not be '" + recordIdentifier + "'");
        if (datestamp == null)
            throw new IllegalArgumentException("parameter datestamp must not be null");
        if (setSpec == null)
            throw new IllegalArgumentException("parameter setSpec must not be null");

        this.recordIdentifier = recordIdentifier;
        this.datestamp = datestamp;
        this.setSpec = setSpec;
        this.statusIsDeleted = statusIsDeleted;
    }

    /**
     * @param recordIdentifier
     *            the unique identifier of an item in a repository
     * @param datestamp
     *            the date of creation, modification or deletion of the record
     * @param statusIsDeleted
     *            true if the header contains the status deleted element
     * @throws IllegalArgumentException
     *             if recordIdentifier or datestamp are whitespace, empty ("")
     *             or null
     */
    public OaiHeader(@NonNull String recordIdentifier, @NonNull Date datestamp, boolean statusIsDeleted) {
        this(recordIdentifier, datestamp, new LinkedList<String>(), statusIsDeleted);
    }

    /**
     * @return the unique identifier of an item in a repository
     */
    @NonNull
    public String getRecordIdentifier() {
        return recordIdentifier;
    }

    /**
     * @return the date of creation, modification or deletion of the record
     */
    @NonNull
    public Date getDatestamp() {
        return datestamp;
    }

    /**
     * @return a list with zero or more elements, each representing the content
     *         of a setSpec element
     */
    @NonNull
    public List<String> getSetSpec() {
        return setSpec;
    }

    /**
     * @return true if the header contains the status deleted element
     */
    public boolean isStatusIsDeleted() {
        return statusIsDeleted;
    }

    @Override
    public String toString() {
        return "OaiHeader [recordIdentifier=" + recordIdentifier + ", datestamp=" + datestamp + ", setSpec=" + setSpec
                + ", statusIsDeleted=" + statusIsDeleted + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((datestamp == null) ? 0 : datestamp.hashCode());
        result = prime * result + ((recordIdentifier == null) ? 0 : recordIdentifier.hashCode());
        result = prime * result + ((setSpec == null) ? 0 : setSpec.hashCode());
        result = prime * result + (statusIsDeleted ? 1231 : 1237);
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
        OaiHeader other = (OaiHeader) obj;
        if (datestamp == null) {
            if (other.datestamp != null)
                return false;
        } else if (!datestamp.equals(other.datestamp))
            return false;
        if (recordIdentifier == null) {
            if (other.recordIdentifier != null)
                return false;
        } else if (!recordIdentifier.equals(other.recordIdentifier))
            return false;
        if (setSpec == null) {
            if (other.setSpec != null)
                return false;
        } else if (!setSpec.equals(other.setSpec))
            return false;
        if (statusIsDeleted != other.statusIsDeleted)
            return false;
        return true;
    }

}
