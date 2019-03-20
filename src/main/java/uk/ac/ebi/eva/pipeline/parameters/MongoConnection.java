/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
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

import com.mongodb.ReadPreference;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Container of credentials for a connection to mongo.
 *
 * The values are injected directly from environment, not from JobParameters.
 */
@Service
public class MongoConnection {

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

    public String getHosts() {
        return hosts;
    }

    public String getAuthenticationDatabase() {
        return authenticationDatabase;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getReadPreferenceName() {
        return readPreference;
    }

    public ReadPreference getReadPreference() {
        return ReadPreference.valueOf(readPreference);
    }
}
