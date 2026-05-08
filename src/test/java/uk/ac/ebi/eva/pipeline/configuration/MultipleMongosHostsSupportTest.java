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

package uk.ac.ebi.eva.pipeline.configuration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.connection.ServerDescription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.test.utils.MongoTestContainerHelper;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
public class MultipleMongosHostsSupportTest extends MongoTestContainerHelper {
    @Test
    public void configShouldReadMultipleHosts() {
        String realHost = mongo.getHost() + ":" + mongo.getMappedPort(27017);
        String uri = "mongodb://" + realHost + ",localhost2:27017/test";
        MongoClient mongoClient = MongoClients.create(uri);
        Set<String> addresses = mongoClient.getClusterDescription()
                .getServerDescriptions()
                .stream()
                .map(ServerDescription::getAddress)
                .map(Object::toString)
                .collect(Collectors.toSet());

        assertTrue(addresses.stream().anyMatch(a -> a.contains(mongo.getHost())));
        assertTrue(addresses.stream().anyMatch(a -> a.contains("localhost2")));
    }
}