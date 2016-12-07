/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.utils;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import org.opencb.commons.utils.CryptoUtils;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Utility class dealing with MongoDB connections using pipeline options
 */
public class MongoDBHelper {

    public static MongoOperations getDefaultMongoOperations(String database) throws UnknownHostException {
        MongoClient mongoClient = new MongoClient();
        mongoClient.setReadPreference(ReadPreference.primary());
        MongoTemplate mongoTemplate = new MongoTemplate(new SimpleMongoDbFactory(mongoClient, database));
        return mongoTemplate;
    }

    public static MongoOperations getMongoOperations(String database, MongoConnection connection) 
            throws UnknownHostException {
        MongoClient client = getMongoClient(connection);
        MongoTemplate mongoTemplate = new MongoTemplate(new SimpleMongoDbFactory(client, database));
        return mongoTemplate;
    }
    
    public static MongoClient getMongoClient(MongoConnection connection) throws UnknownHostException {
        String authenticationDatabase = null;
        String user = null;
        String password = null;
        MongoClient mongoClient;
        
        // The Mongo API is not happy to deal with empty strings for authentication DB, user and password
        if (connection.getAuthenticationDatabase() != null && !connection.getAuthenticationDatabase().trim().isEmpty()) {
            authenticationDatabase = connection.getAuthenticationDatabase();
        }
        if (connection.getUser() != null && !connection.getUser().trim().isEmpty()) {
            user = connection.getUser();
        }
        if (connection.getPassword() != null && !connection.getPassword().trim().isEmpty()) {
            password = connection.getPassword();
        }
        
        if (user == null || password == null) {
            mongoClient = new MongoClient(parseServerAddresses(connection.getHosts()));
        } else {
            mongoClient = new MongoClient(
                    parseServerAddresses(connection.getHosts()),
                    Collections.singletonList(MongoCredential.createCredential(connection.getUser(),
                            authenticationDatabase, connection.getPassword().toCharArray())));
        }
        mongoClient.setReadPreference(getMongoTemplateReadPreferences(connection.getReadPreference()));

        return mongoClient;
    }

    private static List<ServerAddress> parseServerAddresses(String hosts) throws UnknownHostException {
        List<ServerAddress> serverAddresses = new LinkedList<>();
        for (String hostPort : hosts.split(",")) {
            if (hostPort.contains(":")) {
                String[] split = hostPort.split(":");
                Integer port = Integer.valueOf(split[1]);
                serverAddresses.add(new ServerAddress(split[0], port));
            } else {
                serverAddresses.add(new ServerAddress(hostPort, 27017));
            }
        }
        return serverAddresses;
    }


    private static ReadPreference getMongoTemplateReadPreferences(String readPreference) {
        switch (readPreference) {
            case "primary":
                return ReadPreference.primary();
            case "secondary":
                return ReadPreference.secondary();
            default:
                throw new IllegalArgumentException(
                        String.format("%s is not a valid ReadPreference type, please use \"primary\" or \"secondary\"",
                                readPreference));
        }

    }

    /**
     * From org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantConverter
     * #buildStorageId(java.lang.String, int, java.lang.String, java.lang.String)
     * <p>
     * To avoid the initialization of:
     * - DBObjectToVariantSourceEntryConverter
     * - DBObjectToVariantConverter
     */
    public static String buildStorageId(String chromosome, int start, String reference, String alternate) {
        StringBuilder builder = new StringBuilder(chromosome);
        builder.append("_");
        builder.append(start);
        builder.append("_");
        if (!reference.equals("-")) {
            if (reference.length() < 50) {
                builder.append(reference);
            } else {
                builder.append(new String(CryptoUtils.encryptSha1(reference)));
            }
        }

        builder.append("_");
        if (!alternate.equals("-")) {
            if (alternate.length() < 50) {
                builder.append(alternate);
            } else {
                builder.append(new String(CryptoUtils.encryptSha1(alternate)));
            }
        }

        return builder.toString();
    }

    public static String buildStorageFileId(String studyId, String fileId) {
        return studyId + "_" + fileId;
    }

}
