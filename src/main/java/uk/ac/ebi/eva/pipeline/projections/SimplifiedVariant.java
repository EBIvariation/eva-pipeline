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
package uk.ac.ebi.eva.pipeline.projections;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.HgvsMongo;
import uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.VariantAtMongo;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static uk.ac.ebi.eva.commons.mongodb.entities.VariantMongo.ALTERNATE_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.VariantMongo.AT_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.VariantMongo.CHROMOSOME_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.VariantMongo.END_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.VariantMongo.HGVS_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.VariantMongo.LENGTH_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.VariantMongo.REFERENCE_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.VariantMongo.START_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.VariantMongo.TYPE_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.VariantMongo.buildVariantId;
import static uk.ac.ebi.eva.commons.mongodb.entities.VariantMongo.generateAtField;
import static uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.HgvsMongo.createHgvsMongo;

/**
 * Simplified representation of variant to be used when inserting or updating a variant
 */
public class SimplifiedVariant {

    @Id
    private String id;

    @Field(TYPE_FIELD)
    private VariantType variantType;

    @Field(CHROMOSOME_FIELD)
    private String chromosome;

    @Field(START_FIELD)
    private long start;

    @Field(END_FIELD)
    private long end;

    @Field(LENGTH_FIELD)
    private int length;

    @Field(REFERENCE_FIELD)
    private String reference;

    @Field(ALTERNATE_FIELD)
    private String alternate;

    @Field(AT_FIELD)
    private VariantAtMongo at;

    @Field(HGVS_FIELD)
    private Set<HgvsMongo> hgvs;

    SimplifiedVariant() {
        this(null, null, -1, -1, -1, null, null, null);
    }

    public SimplifiedVariant(VariantType variantType, String chromosome, long start, long end, int length,
                             String reference, String alternate, Map<String, Set<String>> hgvs) {
        this.id = buildVariantId(chromosome, start, reference, alternate);
        this.variantType = variantType;
        this.chromosome = chromosome;
        this.start = start;
        this.end = end;
        this.length = length;
        this.reference = reference;
        this.alternate = alternate;
        this.at = generateAtField(chromosome, start);
        this.hgvs = new HashSet<>();
        if (hgvs != null && !hgvs.isEmpty()) {
            this.hgvs.addAll(createHgvsMongo(hgvs));
        }
    }

    public VariantType getVariantType() {
        return variantType;
    }

    public String getChromosome() {
        return chromosome;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public int getLength() {
        return length;
    }

    public String getReference() {
        return reference;
    }

    public String getAlternate() {
        return alternate;
    }

    public VariantAtMongo getAt() {
        return at;
    }

    public Set<HgvsMongo> getHgvs() {
        return hgvs;
    }
}
