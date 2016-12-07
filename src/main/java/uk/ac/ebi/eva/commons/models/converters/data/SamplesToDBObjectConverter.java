/*
 * Copyright 2014-2016 EMBL - European Bioinformatics Institute
 * Copyright 2015 OpenCB
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
package uk.ac.ebi.eva.commons.models.converters.data;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.opencb.biodata.models.feature.Genotype;
import org.springframework.core.convert.converter.Converter;

import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple samples converter that does not require the names of the samples, as it will compress them in mongo.
 * <p>
 * This class is based on OpenCGA MongoDB converters.
 */
public class SamplesToDBObjectConverter implements Converter<VariantSourceEntry, DBObject> {

    @Override
    public DBObject convert(VariantSourceEntry object) {
        Map<Genotype, List<Integer>> genotypeCodes = new HashMap<>();

        // Classify samples by genotype
        for (int i = 0; i < object.getSamplesData().size(); i++) {
            String genotype = object.getSampleData(i).get("GT");
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

        // Get the most common genotype
        Map.Entry<Genotype, List<Integer>> longestList = null;
        for (Map.Entry<Genotype, List<Integer>> entry : genotypeCodes.entrySet()) {
            List<Integer> genotypeList = entry.getValue();
            if (longestList == null || genotypeList.size() > longestList.getValue().size()) {
                longestList = entry;
            }
        }

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
}
