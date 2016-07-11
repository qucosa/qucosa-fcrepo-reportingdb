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

package de.slub.persistence;

import java.util.Date;
import java.util.List;

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;

import de.slub.fedora.oai.OaiHeader;
import de.slub.fedora.oai.OaiRunResult;

public interface PersistenceService {

	/**
	 * @return The details of the last run or null if the database does not
	 *         contain any last run.
	 */
	@Nullable
	public OaiRunResult getLastOaiRunResult();

	/**
	 * @param oaiRunResult
	 *            the data to be stored in database
	 * @return {@code true} if the data was written to database or {@code false}
	 *         if any error occurred while communicating with the database. See
	 *         log for details.
	 */
	public boolean storeOaiRunResult(@NonNull OaiRunResult oaiRunResult);

	/**
	 * Delete all {@link OaiRunResult}s whose
	 * {@link OaiRunResult#getTimestampOfRun()} is older than
	 * oldestResultToKeep. <br />
	 * The most recently inserted {@link OaiRunResult} is never removed, even if
	 * older than the oldestResultToKeep.
	 * 
	 * @param oldestResultToKeep
	 *            the timestamp of run of the oldest OaiRunResult to keep in
	 *            database.
	 * @return {@code true} if the history was cleaned or {@code false} if any
	 *         error occurred while communicating with the database. See log for
	 *         details.
	 */
	public boolean cleanupOaiRunResults(@NonNull Date oldestResultToKeep);

	/**
	 * Adds the {@link OaiHeader}s to database. If the database already contains
	 * a row that matches {@link OaiHeader#getRecordIdentifier()}, this row is
	 * updated.
	 * 
	 * @param headers
	 *            {@link OaiHeader}s to add or update in database.
	 * @return {@code true} if all headers have been added/updated successfully,
	 *         {@code false} otherwise. See log for details.
	 */
	public boolean addOrUpdateHeaders(@NonNull List<OaiHeader> headers);

	/**
	 * Get {@link OaiHeader}s from database. At most 1000 headers are returned.
	 * 
	 * @return
	 */
	@NonNull
	public List<OaiHeader> getOaiHeaders();

	/**
	 * Deletes the {@link OaiHeader}s if all of their fields' values match the
	 * values stored in database. If, e.g.,
	 * {@link OaiHeader#getRecordIdentifier()} matches a row in database, but
	 * one of the other values differs, the row is not deleted and the
	 * {@link OaiHeader} is returned to the caller.
	 * 
	 * @param headersToRemove
	 *            {@link OaiHeader}s to be removed if unmodified.
	 * @return the headers that were requested to be deleted but have not been
	 *         removed from db. This {@link List} is always a subset of
	 *         {@code headersToRemove}.
	 */
	@NonNull
	public List<OaiHeader> removeOaiHeadersIfUnmodified(@NonNull List<OaiHeader> headersToRemove);

}
