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

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QucosaDocumentFilter extends OaiHeaderFilter {

	private static final String ACCEPTED_QUCOSA_ID_REG_EX = ".+qucosa:\\d+";
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public List<OaiHeader> filterOaiHeaders(List<OaiHeader> oaiHeaders) {

		List<OaiHeader> acceptedHeaders = new LinkedList<>();

		for (OaiHeader header : oaiHeaders) {
			String id = header.getRecordIdentifier();

			if (id.matches(ACCEPTED_QUCOSA_ID_REG_EX)) {

				// TODO do we still have concurrentmodificatioExceptions when
				// deleting? If so, is the exception caused because we are
				// removing an object from the list that is iterated over? If
				// so, copy it locally and traverse the copy while removing from
				// original list?
				acceptedHeaders.add(header);
			} else {
				logger.debug("Removing header with id '{}'", id);
			}
		}

		return acceptedHeaders;

	}

	private String getLocalIdentifier(String oaiId) {
		return oaiId.substring(oaiId.indexOf(':', "oai:".length()) + 1);
	}

}
