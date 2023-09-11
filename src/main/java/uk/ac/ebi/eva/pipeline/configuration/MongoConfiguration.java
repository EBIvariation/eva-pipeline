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
package uk.ac.ebi.eva.pipeline.configuration;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import org.opencb.datastore.core.config.DataStoreServerAddress;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.opencb.opencga.storage.mongodb.variant.VariantMongoDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.WriteResultChecking;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import uk.ac.ebi.eva.commons.mongodb.utils.MongoUtils;
import uk.ac.ebi.eva.pipeline.parameters.DatabaseParameters;
import uk.ac.ebi.eva.pipeline.parameters.MongoConnectionDetails;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Utility class dealing with MongoDB connections using pipeline options
 */
@Configuration
public class MongoConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(MongoConfiguration.class);

    @Bean
    public MongoMappingContext mongoMappingContext() {
        return new MongoMappingContext();
    }

    @Bean
    @StepScope
    public MongoTemplate mongoTemplate(DatabaseParameters databaseParameters, MongoMappingContext mongoMappingContext)
            throws UnknownHostException, UnsupportedEncodingException {
        return getMongoTemplate(databaseParameters.getDatabaseName(), databaseParameters.getMongoConnectionDetails(),
                mongoMappingContext);
    }

    @Bean
    @StepScope
    public MongoClient mongoClient(DatabaseParameters databaseParameters) throws UnsupportedEncodingException{
        MongoClient client = new MongoClient(constructMongoClientURI(databaseParameters.getDatabaseName(),
                databaseParameters.getMongoConnectionDetails()));
        client.setWriteConcern(WriteConcern.MAJORITY);
        return client;
    }

    public static MongoTemplate getMongoTemplate(String databaseName, MongoConnectionDetails mongoConnectionDetails,
                                                 MongoMappingContext mongoMappingContext)
            throws UnknownHostException, UnsupportedEncodingException {
        MongoClientURI uri = constructMongoClientURI(databaseName, mongoConnectionDetails);
        MongoDbFactory mongoFactory = new SimpleMongoDbFactory(uri);
        MappingMongoConverter mappingMongoConverter = getMappingMongoConverter(mongoFactory, mongoMappingContext);
        MongoTemplate mongoTemplate = new MongoTemplate(mongoFactory, mappingMongoConverter);
        mongoTemplate.setWriteConcern(WriteConcern.MAJORITY);
        mongoTemplate.setWriteResultChecking(WriteResultChecking.EXCEPTION);
        return mongoTemplate;
    }

    public static MongoClientURI constructMongoClientURI(String databaseName,
                                                         MongoConnectionDetails mongoConnectionDetails)
            throws UnsupportedEncodingException {
        return MongoUtils.constructMongoClientURI(mongoConnectionDetails.getHosts(),
                                                  null,
                                                  databaseName,
                                                  mongoConnectionDetails.getUser(),
                                                  mongoConnectionDetails.getPassword(),
                                                  mongoConnectionDetails.getAuthenticationDatabase(),
                                                  mongoConnectionDetails.getAuthenticationMechanism(),
                                                  mongoConnectionDetails.getReadPreferenceName());
    }

    private static MappingMongoConverter getMappingMongoConverter(MongoDbFactory mongoFactory,
                                                                  MongoMappingContext mongoMappingContext) {
        DbRefResolver dbRefResolver = new DefaultDbRefResolver(mongoFactory);
        MappingMongoConverter mongoConverter = new MappingMongoConverter(dbRefResolver, mongoMappingContext);
        mongoConverter.setTypeMapper(new DefaultMongoTypeMapper(null));

        // Customization: replace dots with pound sign
        mongoConverter.setMapKeyDotReplacement("£");

        return mongoConverter;
    }

    public static VariantDBAdaptor getDbAdaptor(DatabaseParameters dbParameters) throws UnknownHostException, IllegalOpenCGACredentialsException {
        MongoCredentials credentials = getMongoCredentials(dbParameters);
        String variantsCollectionName = dbParameters.getCollectionVariantsName();
        String filesCollectionName = dbParameters.getCollectionFilesName();

        logger.debug("Getting DBAdaptor to database '{}'", credentials.getMongoDbName());
        return new VariantMongoDBAdaptor(credentials, variantsCollectionName, filesCollectionName);
    }

    private static MongoCredentials getMongoCredentials(DatabaseParameters dbParameters) throws IllegalOpenCGACredentialsException {
        MongoConnectionDetails mongoConnection = dbParameters.getMongoConnectionDetails();
        String hosts = mongoConnection.getHosts();
        List<DataStoreServerAddress> dataStoreServerAddresses = MongoCredentials.parseDataStoreServerAddresses(hosts);

        String dbName = dbParameters.getDatabaseName();
        String user = mongoConnection.getUser();
        String pass = mongoConnection.getPassword();
        String authenticationMechanism = mongoConnection.getAuthenticationMechanism();

        MongoCredentials mongoCredentials = new MongoCredentials(dataStoreServerAddresses, dbName, user, pass);
        mongoCredentials.setAuthenticationDatabase(mongoConnection.getAuthenticationDatabase());
        mongoCredentials.setAuthenticationMechanism(authenticationMechanism);
        return mongoCredentials;
    }

}
