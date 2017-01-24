/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.pipeline.parameters;

import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import uk.ac.ebi.eva.utils.MongoConnection;

/**
 * Service that holds access to the values for database. This include the configuration
 * values for database connection that are got as values not parameters.
 */
@Service
@JobScope
public class DatabaseParameters {

    private static final String PARAMETER = "#{jobParameters['";
    private static final String END = "']}";

    @Value(PARAMETER + JobParametersNames.DB_NAME + END)
    private String databaseName;

    @Value(PARAMETER + JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME + END)
    private String collectionVariantsName;

    @Value(PARAMETER + JobParametersNames.DB_COLLECTIONS_FILES_NAME + END)
    private String collectionFilesName;

    @Value("${" + JobParametersNames.CONFIG_DB_HOSTS + ":#{null}}")
    private String hosts;

    @Value("${" + JobParametersNames.CONFIG_DB_AUTHENTICATIONDB + ":#{null}}")
    private String authenticationDatabase;

    @Value("${" + JobParametersNames.CONFIG_DB_USER + ":#{null}}")
    private String user;

    @Value("${" + JobParametersNames.CONFIG_DB_PASSWORD + ":#{null}}")
    private String password;

    @Value("${" + JobParametersNames.CONFIG_DB_READPREFERENCE + ":#{null}}")
    private String readPreference;

    public MongoConnection getMongoConnection() {
        return new MongoConnection(hosts, authenticationDatabase, user, password, readPreference);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getCollectionVariantsName() {
        return collectionVariantsName;
    }

    public String getCollectionFilesName() {
        return collectionFilesName;
    }
}
