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
package embl.ebi.variation.eva.pipeline;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jmmut on 2016-07-19.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class ConnectionHelper {
    public static List<ServerAddress> parseServerAddresses(String hosts) throws UnknownHostException {
        List<ServerAddress> erverAddresses = new LinkedList<>();
        for (String hostPort : hosts.split(",")) {
            if (hostPort.contains(":")) {
                String[] split = hostPort.split(":");
                Integer port = Integer.valueOf(split[1]);
                erverAddresses.add(new ServerAddress(split[0], port));
            } else {
                erverAddresses.add(new ServerAddress(hostPort, 27017));
            }
        }
        return erverAddresses;
    }

    public static MongoTemplate getMongoTemplate(String hosts, String authenticationDB, String database,
                                                 String user, char[] password)
            throws UnknownHostException {
        return new MongoTemplate(
                new SimpleMongoDbFactory(
                        new MongoClient(
                                parseServerAddresses(hosts),
                                Collections.singletonList(MongoCredential.createCredential(
                                        user,
                                        authenticationDB,
                                        password
                                        )
                                )
                        ),
                        database
                )
        );
    }
}
