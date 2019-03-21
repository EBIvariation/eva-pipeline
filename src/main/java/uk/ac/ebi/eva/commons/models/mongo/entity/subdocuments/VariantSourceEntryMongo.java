/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments;

import com.mongodb.BasicDBObject;
import org.opencb.biodata.models.feature.Genotype;
import org.springframework.data.mongodb.core.mapping.Field;
import uk.ac.ebi.eva.utils.CompressionHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Mongo database representation of Variant Source entry.
 */
public class VariantSourceEntryMongo {

    public final static char CHARACTER_TO_REPLACE_DOTS = (char) 163; // <-- £

    public final static String FILEID_FIELD = "fid";

    public final static String STUDYID_FIELD = "sid";

    public final static String ALTERNATES_FIELD = "alts";

    public final static String ATTRIBUTES_FIELD = "attrs";

    public final static String FORMAT_FIELD = "fm";

    public final static String SAMPLES_FIELD = "samp";

    @Field(FILEID_FIELD)
    private String fileId;

    @Field(STUDYID_FIELD)
    private String studyId;

    @Field(ALTERNATES_FIELD)
    private String[] alternates;

    @Field(ATTRIBUTES_FIELD)
    private BasicDBObject attrs;

    @Field(FORMAT_FIELD)
    private String format;

    @Field(SAMPLES_FIELD)
    private BasicDBObject samp;

    VariantSourceEntryMongo() {
        // Spring empty constructor
    }

    public VariantSourceEntryMongo(String fileId, String studyId, String[] alternates, Map<String, String> attributes) {
        this.fileId = fileId;
        this.studyId = studyId;
        if (alternates != null && alternates.length > 0) {
            this.alternates = new String[alternates.length];
            System.arraycopy(alternates, 0, this.alternates, 0, alternates.length);
        }
        attrs = buildAttributes(attributes);

        this.format = null;
        this.samp = null;
    }

    public VariantSourceEntryMongo(String fileId, String studyId, String[] alternates, Map<String, String>
            attributes, String format, List<Map<String, String>> samplesData) {
        this(fileId, studyId, alternates, attributes);
        this.format = format;
        this.samp = buildSampleData(samplesData);
    }

    private BasicDBObject buildSampleData(List<Map<String, String>> samplesData) {
        Map<Genotype, List<Integer>> genotypeCodes = classifySamplesByGenotype(samplesData);

        // Get the most common genotype
        Map.Entry<Genotype, List<Integer>> longestList = getLongestGenotypeList(genotypeCodes);

        // In Mongo, samples are stored in a map, classified by their genotype.
        // The most common genotype will be marked as "default" and the specific
        // positions where it is shown will not be stored. Example from 1000G:
        // "def" : 0|0,
        // "0|1" : [ 41, 311, 342, 358, 881, 898, 903 ],
        // "1|0" : [ 262, 290, 300, 331, 343, 369, 374, 391, 879, 918, 930 ]
        BasicDBObject mongoSamples = new BasicDBObject();
        for (Map.Entry<Genotype, List<Integer>> entry : genotypeCodes.entrySet()) {
            String genotypeStr = entry.getKey().toString().replace(".", "-1");
            if (longestList != null && entry.getKey().equals(longestList.getKey())) {
                mongoSamples.append("def", genotypeStr);
            } else {
                mongoSamples.append(genotypeStr, entry.getValue());
            }
        }

        return mongoSamples;
    }

    private Map.Entry<Genotype, List<Integer>> getLongestGenotypeList(Map<Genotype, List<Integer>> genotypeCodes) {
        Map.Entry<Genotype, List<Integer>> longestList = null;
        for (Map.Entry<Genotype, List<Integer>> entry : genotypeCodes.entrySet()) {
            List<Integer> genotypeList = entry.getValue();
            if (longestList == null || genotypeList.size() > longestList.getValue().size()) {
                longestList = entry;
            }
        }
        return longestList;
    }

    private Map<Genotype, List<Integer>> classifySamplesByGenotype(List<Map<String, String>> samplesData) {
        Map<Genotype, List<Integer>> genotypeCodes = new HashMap<>();

        for (int i = 0; i < samplesData.size(); i++) {
            String genotype = samplesData.get(i).get("GT");
            if (genotype != null) {
                Genotype g = new Genotype(genotype);
                List<Integer> samplesWithGenotype = genotypeCodes.get(g);
                if (samplesWithGenotype == null) {
                    samplesWithGenotype = new ArrayList<>();
                    genotypeCodes.put(g, samplesWithGenotype);
                }
                samplesWithGenotype.add(i);
            }
        }
        return genotypeCodes;
    }

    private BasicDBObject buildAttributes(Map<String, String> attributes) {
        BasicDBObject attrs = null;
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            Object value = entry.getValue();
            if (entry.getKey().equals("src")) {
                String[] fields = entry.getValue().split("\t");
                StringBuilder sb = new StringBuilder();
                sb.append(fields[0]);
                for (int i = 1; i < fields.length && i < 8; i++) {
                    sb.append("\t").append(fields[i]);
                }
                try {
                    value = CompressionHelper.gzip(sb.toString());
                } catch (IOException ex) {
                    Logger.getLogger(VariantSourceEntryMongo.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

            if (attrs == null) {
                attrs = new BasicDBObject(entry.getKey().replace('.', CHARACTER_TO_REPLACE_DOTS), value);
            } else {
                attrs.append(entry.getKey().replace('.', CHARACTER_TO_REPLACE_DOTS), value);
            }
        }
        return attrs;
    }

}
