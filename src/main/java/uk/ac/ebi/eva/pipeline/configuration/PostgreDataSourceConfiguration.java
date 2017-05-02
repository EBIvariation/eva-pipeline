/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import uk.ac.ebi.eva.pipeline.Application;

import javax.sql.DataSource;

/**
 * Configuration will be loaded from the file "application-production.properties".
 */
@Configuration
@Profile(Application.PRODUCTION_PROFILE)
public class PostgreDataSourceConfiguration {

    @Bean
    @Primary
    public DataSource postgreDataSource(Environment env) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(env.getProperty("job.repository.driverClassName"));
        dataSource.setUrl(env.getProperty("job.repository.url"));
        dataSource.setUsername(env.getProperty("job.repository.username"));
        dataSource.setPassword(env.getProperty("job.repository.password"));
        return dataSource;
    }

}
