/*
 * Copyright 2015 Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>.
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
package embl.ebi.variation.eva.pipeline.configuration;

import javax.sql.DataSource;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

@Configuration
@EnableBatchProcessing
@PropertySource("classpath:datasource.properties")
public class PostgreInfrastructureConfiguration implements InfrastructureConfiguration {

    @Autowired
    private Environment env;

    @Override
    public DataSource dataSource() {        
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
//        dataSource.setDriverClassName("org.postgresql.Driver");
//        dataSource.setUrl("jdbc:postgresql://10.7.248.34:49153/spring_batch_annotations");
//        dataSource.setUsername("postgres");
//        dataSource.setPassword("batch");
//        logger.info("using as repository url: " + pipelineConfig.jobRepositoryUrl);
        
        dataSource.setDriverClassName(env.getProperty("jobRepositoryDriverClassName"));
        dataSource.setUrl(env.getProperty("jobRepositoryUrl"));
        dataSource.setUsername(env.getProperty("jobRepositoryUsername"));
        dataSource.setPassword(env.getProperty("jobRepositoryPassword"));
        
        return dataSource;
    }
    
//    @Bean
//    public DataSource dataSource() {
//        try {
//            InitialContext initialContext = new InitialContext();
//            return (DataSource) initialContext.lookup(env.getProperty("datasource.jndi"));
//        } catch (NamingException e) {
//            throw new RuntimeException("JNDI lookup failed.", e);
//        }
//    }
//
//    public JobRepository getJobRepository() throws Exception {
//        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
//        factory.setDataSource(dataSource());
//        factory.setTransactionManager(getTransactionManager());
//        factory.afterPropertiesSet();
//        return (JobRepository) factory.getObject();
//    }
//
//    public PlatformTransactionManager getTransactionManager() throws Exception {
//        return new WebSphereUowTransactionManager();
//    }
//
//    public JobLauncher getJobLauncher() throws Exception {
//        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
//        jobLauncher.setJobRepository(getJobRepository());
//        jobLauncher.afterPropertiesSet();
//        return jobLauncher;
//    }

}
