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
package uk.ac.ebi.eva.pipeline.io.mappers;

import org.springframework.batch.item.file.LineMapper;

import uk.ac.ebi.eva.pipeline.model.FeatureCoordinates;

import java.util.Map;
import java.util.TreeMap;

/**
 * Maps a line in a GTF file to a FeatureCoordinates.
 *
 * input: GTF line. example:
 * 8	ensembl	gene	183180	246703	.	+	.	gene_id "ENSCSAG00000017073"; gene_version "1"; gene_name "FBXO25"; gene_source "ensembl"; gene_biotype "protein_coding";
 *
 * output: a FeatureCoordinates bean properly filled:
 *  class FeatureCoordinates {
 *      String id = "ENSCSAG00000017073"
 *      String name = "FBXO25"
 *      String feature = "gene"
 *      String chromosome = "8"
 *      int start = 183180
 *      int end = 246703
 *  }
 */
public class GeneLineMapper implements LineMapper<FeatureCoordinates> {
    @Override
    public FeatureCoordinates mapLine(String line, int lineNumber) throws Exception {
        String[] lineSplit = line.split("\t");
        String[] attributesSplit = lineSplit[8].split(";");
        Map<String, String> attributes = new TreeMap<>();
        for (String attribute : attributesSplit) {
            String[] keyValue = attribute.split(" ");

            // don't do a `put(keyValue[0], keyValue[1])`: a space may appear before the key
            // also, remove quotes from the value
            int valueLength = keyValue[keyValue.length - 1].length();
            attributes.put(keyValue[keyValue.length - 2], keyValue[keyValue.length - 1].substring(1, valueLength - 1));
        }

        String feature = lineSplit[2];
        return new FeatureCoordinates(attributes.get(feature + "_id"), attributes.get(feature + "_name"), feature,
                lineSplit[0], Integer.parseInt(lineSplit[3]), Integer.parseInt(lineSplit[4]));
    }

}
