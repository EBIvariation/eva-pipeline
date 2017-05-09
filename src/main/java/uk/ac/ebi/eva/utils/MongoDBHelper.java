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
package uk.ac.ebi.eva.utils;

import com.mongodb.ServerAddress;
import org.opencb.commons.utils.CryptoUtils;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.mongo.documents.Annotation;

import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;


public class MongoDBHelper {

    public static final String BACKGROUND_INDEX = "background";

    public static final String UNIQUE_INDEX = "unique";

    public static final String INDEX_NAME = "name";

    private MongoDBHelper() {
        // Can't be instantiated
    }

    public static List<ServerAddress> parseServerAddresses(String hosts) throws UnknownHostException {
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

}
