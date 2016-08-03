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

package de.slub.fedora.reporting;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;

public class ReportingProperties {

    private static final String PROPERTIES_FILE_NAME = "/reporting.properties";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static ReportingProperties instance;
    private final Properties props = new Properties();

    private ReportingProperties() {
        try (InputStream in = this.getClass().getResourceAsStream(PROPERTIES_FILE_NAME);
                Reader reader = new InputStreamReader(in, "UTF-8")) {

            props.load(reader);
            // FIXME: how to make sure all properties can also be converted to
            // types that are required? Shouldn't thins be checked right now?

        } catch (IOException ex) {
            logger.error(MarkerFactory.getMarker("FATAL"),
                    "Fatal error. Can not load configuration from properties file '{}'."
                            + " There is no default. Nothing can be daone without configuration parameter."
                            + "Exiting. Exception details: {}",
                    PROPERTIES_FILE_NAME, ex);
            System.exit(1);
        }
    }

    protected static ReportingProperties getInstance() {
        if (ReportingProperties.instance == null) {
            instance = new ReportingProperties();
        }
        return instance;
    }

    protected String getPostgreSQLDatabaseURL() {
        return props.getProperty("db.url");
    }

    protected String getPostgreSQLUser() {
        return props.getProperty("db.user");
    }

    protected String getPostgreSQLPasswd() {
        return props.getProperty("db.passwd");
    }

    protected String getOaiDataProviderURL() {
        return props.getProperty("oai.url");
    }

    protected int getOaiDataProviderPollingInterval() {
        return Integer.parseInt(props.getProperty("oai.pollseconds"));
    }

    protected boolean getFC3CompatibilityMode() {
        return Boolean.parseBoolean(props.getProperty("oai.fc3compatibility"));
    }

    protected Duration getOaiRunResultHistoryLength() {
        return Duration.standardHours(Long.parseLong(props.getProperty("oai.runresulthistorylengthhours")));
    }
}
