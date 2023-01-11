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
package uk.ac.ebi.eva.pipeline.io.mappers;

import org.springframework.batch.item.file.LineMapper;
import uk.ac.ebi.eva.commons.models.data.Variant;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class AccessionReportLineMapper implements LineMapper<Variant> {
    public AccessionReportLineMapper() {
    }

    @Override
    public Variant mapLine(String line, int lineNumber) {
        String[] fields = line.split("\t");
        if (fields.length < 5) {
            throw new IllegalArgumentException("Not enough fields provided (min 5)");
        }

        String chromosome = fields[0];
        int position = Integer.parseInt(fields[1]);
        Set<String> ids = getIds(fields);
        String reference = fields[3];
        String alternate = fields[4];

        Variant variant = new Variant();
        variant.setChromosome(chromosome);
        variant.setStart(position);
        variant.setReference(reference);
        variant.setAlternate(alternate);
        variant.setIds(ids);

        return variant;
    }

    private Set<String> getIds(String[] fields) {
        Set<String> ids = new HashSet<>();
        if (!fields[2].equals(".")) {
            ids.addAll(Arrays.asList(fields[2].split(";")));
        }
        return ids;
    }
}
