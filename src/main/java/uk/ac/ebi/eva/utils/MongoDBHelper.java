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
import uk.ac.ebi.eva.commons.models.data.Annotation;

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

    public static String buildVariantStorageId(Variant v) {
        return buildVariantStorageId(v.getChromosome(), v.getStart(), v.getReference(), v.getAlternate());
    }

    public static String buildVariantStorageId(Annotation va) {
        return buildVariantStorageId(va.getChromosome(), va.getStart(), va.getReferenceAllele(), va.getAlternativeAllele());
    }

    /**
     * From org.opencb.opencga.storage.mongodb.variant.VariantToDBObjectConverter
     * #buildVariantStorageId(java.lang.String, int, java.lang.String, java.lang.String)
     */
    public static String buildVariantStorageId(String chromosome, int start, String reference, String alternate) {
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

    public static String buildAnnotationStorageId(String chromosome,
                                                  int start,
                                                  String reference,
                                                  String alternate,
                                                  String vepVersion,
                                                  String vepCacheVersion) {
        StringBuilder builder = new StringBuilder(buildVariantStorageId(chromosome, start, reference, alternate));
        builder.append("_");
        builder.append(vepVersion);
        builder.append("_");
        builder.append(vepCacheVersion);

        return builder.toString();
    }

}
