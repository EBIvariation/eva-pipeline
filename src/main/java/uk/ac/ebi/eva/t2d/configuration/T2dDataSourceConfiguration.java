/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.t2d.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import uk.ac.ebi.eva.pipeline.Application;

import javax.persistence.PersistenceContext;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

import static uk.ac.ebi.eva.t2d.utils.PropertyUtils.flatten;

/**
 * Database configuration
 */
@Configuration
@Profile(Application.T2D_PROFILE)
@EnableTransactionManagement
@EnableJpaRepositories(
        basePackages = "uk.ac.ebi.eva.t2d.repository",
        entityManagerFactoryRef = "t2dEntityManagerFactory",
        transactionManagerRef = "t2dTransactionManager")
@EnableConfigurationProperties
public class T2dDataSourceConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(T2dDataSourceConfiguration.class);
    public static final String T2D_PERSISTENCE_UNIT = "t2dPersistenceUnit";
    private static final String T2D_DATASOURCE_PREFIX = "t2d.datasource";
    private static final String T2D_JPA_PROPERTIES_PREFIX = "t2d.jpa.properties";
    private static final String ENTITY_BASE_PACKAGE = "uk.ac.ebi.eva.t2d.entity";
    public static final String T2D_TRANSACTION_MANAGER = "t2dTransactionManager";

    @Bean
    @ConfigurationProperties(prefix = T2D_DATASOURCE_PREFIX)
    DataSource t2dDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    @PersistenceContext(unitName = T2D_PERSISTENCE_UNIT)
    LocalContainerEntityManagerFactoryBean t2dEntityManagerFactory() {
        Map<String, Object> properties = flatten(t2dHibernateProperties());

        LocalContainerEntityManagerFactoryBean factoryBean = new LocalContainerEntityManagerFactoryBean();
        factoryBean.setDataSource(t2dDataSource());
        factoryBean.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
        factoryBean.setPackagesToScan(new String[]{ENTITY_BASE_PACKAGE});
        factoryBean.setJpaPropertyMap(properties);
        factoryBean.setPersistenceUnitName(T2D_PERSISTENCE_UNIT);
        return factoryBean;
    }

    @Bean(T2D_TRANSACTION_MANAGER)
    PlatformTransactionManager t2dTransactionManager() {
        JpaTransactionManager transactionManager = new JpaTransactionManager();
        transactionManager.setEntityManagerFactory(t2dEntityManagerFactory().getObject());
        transactionManager.setDataSource(t2dDataSource());
        return transactionManager;
    }

    @Bean
    @ConfigurationProperties(prefix = T2D_JPA_PROPERTIES_PREFIX)
    Map<String, Object> t2dHibernateProperties() {
        return new HashMap<>();
    }

}
