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
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import uk.ac.ebi.eva.pipeline.parameters.DatabaseParameters;
import uk.ac.ebi.eva.pipeline.parameters.MongoConnectionDetails;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
    public MongoOperations mongoTemplate(DatabaseParameters databaseParameters, MongoMappingContext mongoMappingContext)
            throws UnknownHostException, UnsupportedEncodingException {
        return getMongoOperations(databaseParameters.getDatabaseName(), databaseParameters.getMongoConnectionDetails(),
                mongoMappingContext);
    }

    @Bean
    @StepScope
    public MongoClient mongoClient(DatabaseParameters databaseParameters) throws UnsupportedEncodingException{
        return new MongoClient(constructMongoClientURI(databaseParameters.getDatabaseName(),
                databaseParameters.getMongoConnectionDetails()));
    }

    public static MongoOperations getMongoOperations(String databaseName, MongoConnectionDetails mongoConnectionDetails,
                                                     MongoMappingContext mongoMappingContext)
            throws UnknownHostException, UnsupportedEncodingException {
        MongoClientURI uri = constructMongoClientURI(databaseName, mongoConnectionDetails);
        MongoDbFactory mongoFactory = new SimpleMongoDbFactory(uri);
        MappingMongoConverter mappingMongoConverter = getMappingMongoConverter(mongoFactory, mongoMappingContext);
        return new MongoTemplate(mongoFactory, mappingMongoConverter);
    }

    public static MongoClientURI constructMongoClientURI(String databaseName,
                                                         MongoConnectionDetails mongoConnectionDetails)
            throws UnsupportedEncodingException {
        List<String> options = new ArrayList<>();
        if (!mongoConnectionDetails.getAuthenticationDatabase().isEmpty()) {
            options.add(String.format("authSource=%s", mongoConnectionDetails.getAuthenticationDatabase()));
        }
        if (!mongoConnectionDetails.getAuthenticationMechanism().isEmpty()) {
            options.add(String.format("authMechanism=%s", mongoConnectionDetails.getAuthenticationMechanism()));
        }
        if (Objects.nonNull(mongoConnectionDetails.getReadPreference())) {
            options.add(String.format("readPreference=%s", mongoConnectionDetails.getReadPreference().toString()));
        }
        String uri = String.format("mongodb://%s:%s@%s/%s", mongoConnectionDetails.getUser(),
                URLEncoder.encode(mongoConnectionDetails.getPassword(), StandardCharsets.UTF_8.toString()),
                mongoConnectionDetails.getHosts(), databaseName);
        if(!options.isEmpty()) {
            uri += "?" + String.join("&", options);
        }
        return new MongoClientURI(uri);
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
