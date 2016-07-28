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
	 * @return The details of the last run or {@coce null} if there isnt any
	 *         last run.
	 */
	@Nullable
	public OaiRunResult getLastOaiRunResult();

	/**
	 * @param oaiRunResult
	 *            the data to be persisted
	 * @throws PersistenceException
	 *             if any error occurred.
	 */
	public void storeOaiRunResult(@NonNull OaiRunResult oaiRunResult) throws PersistenceException;

	/**
	 * Delete all {@link OaiRunResult}s whose
	 * {@link OaiRunResult#getTimestampOfRun()} is older than
	 * oldestResultToKeep. <br />
	 * The most recently inserted {@link OaiRunResult} is never removed, even if
	 * older than the oldestResultToKeep.
	 * 
	 * @param oldestResultToKeep
	 *            the timestamp of run of the oldest OaiRunResult to keep.
	 * @throws PersistenceException
	 *             if any error occurred.
	 */
	public void cleanupOaiRunResults(@NonNull Date oldestResultToKeep) throws PersistenceException;

	/**
	 * Persist all {@link OaiHeader}s. If the persistence layer already contains
	 * a {@link OaiHeader} object that matches
	 * {@link OaiHeader#getRecordIdentifier()}, this object updated.
	 * 
	 * @param headers
	 *            {@link OaiHeader}s to add or update.
	 * @throws PersistenceException
	 *             if any error occurred.
	 */
	public void addOrUpdateHeaders(@NonNull List<OaiHeader> headers) throws PersistenceException;

	/**
	 * Get {@link OaiHeader}s from persistence. At most 1000 headers are
	 * returned.
	 * 
	 * @return
	 * @throws PersistenceException
	 *             if any error occurred.
	 */
	@NonNull
	public List<OaiHeader> getOaiHeaders() throws PersistenceException;

	/**
	 * Deletes the {@link OaiHeader}s. An {@link OaiHeader} is removed from
	 * persistence iff it equals an object in the {@code headersToRemove}. All
	 * objects from {@code headersToRemove} that are not found in persistence
	 * are returned to the caller.
	 * 
	 * @param headersToRemove
	 *            {@link OaiHeader}s to be removed if unmodified.
	 * @return the headers that were requested to be deleted but have not been
	 *         removed from persistence. This {@link List} is always a subset of
	 *         {@code headersToRemove}.
	 * @throws PersistenceException
	 *             if any error occurred.
	 */
	@NonNull
	public List<OaiHeader> removeOaiHeadersIfUnmodified(@NonNull List<OaiHeader> headersToRemove)
			throws PersistenceException;

}
