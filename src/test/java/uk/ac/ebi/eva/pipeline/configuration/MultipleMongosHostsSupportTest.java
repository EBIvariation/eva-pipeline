/*
 * Copyright 2023 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.pipeline.io.readers;

import com.mongodb.ServerAddress;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.parameters.MongoConnection;
import uk.ac.ebi.eva.test.configuration.MongoOperationConfiguration;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

/**
 * {@link VariantsMongoReader}
 * input: a variants collection address
 * output: a DBObject each time `.read()` is called, with at least: chr, start, annot
 */
@RunWith(SpringRunner.class)
@ActiveProfiles(Application.VARIANT_ANNOTATION_MONGO_PROFILE)
@TestPropertySource({"classpath:test-multiple-mongo-hosts.properties"})
@ContextConfiguration(classes = {MongoConnection.class, MongoMappingContext.class, MongoOperationConfiguration.class})
public class MultipleMongosHostsSupportTest {
    @Autowired
    private MongoTemplate mongoTemplate;

    @Test
    public void configShouldReadMultipleHosts() throws Exception {
        Set<String> addresses = mongoTemplate.getMongoDbFactory().getLegacyDb()
                .getMongo().getAllAddress().stream().map(ServerAddress::toString).collect(Collectors.toSet());        
        assertTrue(addresses.contains("|eva.mongo.host.test|:27017"));
        assertTrue(addresses.contains("localhost2:27017"));
    }
}
