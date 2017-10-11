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

package de.qucosa.fedora.reporting;

import org.joda.time.Duration;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;

public class ReportingProperties {

    private static final String DEFAULT_PROPERTIES_FILE = "/default.properties";
    private static final String LOCAL_PROPERTIES_FILE = "/local.properties";
    private static final String PROPERTIES_FILE_FORMAT = "ISO-8859-1";
    private static final String XML_PREFIX_IDENTIFIER = "mets.namespace.";

    private static ReportingProperties instance;

    private final Properties props = new Properties();

    private ReportingProperties() throws IOException {
        try (InputStream in = getClass().getResourceAsStream(DEFAULT_PROPERTIES_FILE);
             Reader reader = new InputStreamReader(in, PROPERTIES_FILE_FORMAT)) {
            props.load(reader);
        }
        overwriteWithLocalProperties();
        overwriteWithSystemProperties();
    }

    public static ReportingProperties getInstance() throws IOException {
        if (ReportingProperties.instance == null) {
            instance = new ReportingProperties();
        }
        return instance;
    }
    
    private void overwriteWithLocalProperties() throws IOException {
            try (InputStream in = getClass().getResourceAsStream(LOCAL_PROPERTIES_FILE)) {

                // file local.properties is optional, it may not exist  
                if (in != null) {
                    try (Reader reader = new InputStreamReader(in, PROPERTIES_FILE_FORMAT)) {
                        props.load(reader);                        
                    }
                }
            }
    }

    private void overwriteWithSystemProperties() {
        for (Object o : System.getProperties().keySet()) {
            String key = (String) o;
            if (key.startsWith("db.") || key.startsWith("oai.") || key.startsWith("mets.")) {
                props.setProperty(key, System.getProperty(key));
            }
        }
    }

    public String getPostgreSQLDatabaseURL() {
        return props.getProperty("db.url");
    }

    public String getPostgreSQLDriver() {
        return props.getProperty("db.driver");
    }
    
    public String getPostgreSQLUser() {
        return props.getProperty("db.user");
    }

    public String getPostgreSQLPasswd() {
        return props.getProperty("db.passwd");
    }

    public String getOaiDataProviderURL() {
        return props.getProperty("oai.url");
    }

    public int getOaiDataProviderPollingInterval() {
        return Integer.parseInt(props.getProperty("oai.pollseconds"));
    }

    public boolean getFC3CompatibilityMode() {
        return Boolean.parseBoolean(props.getProperty("oai.fc3compatibility"));
    }

    public Duration getOaiRunResultHistoryLength() {
        return Duration.standardHours(Long.parseLong(props.getProperty("oai.runresulthistorylengthhours")));
    }  

    public String getMetsDisseminationURL() {
        return props.getProperty("mets.url");
    }

    public int getMetsDisseminationPollingInterval() {
        return Integer.parseInt(props.getProperty("mets.pollseconds"));
    }
}
