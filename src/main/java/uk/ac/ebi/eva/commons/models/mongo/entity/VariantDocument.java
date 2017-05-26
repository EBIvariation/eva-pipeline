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
package uk.ac.ebi.eva.commons.models.mongo.entity;

import org.opencb.commons.utils.CryptoUtils;
import org.opencb.opencga.storage.mongodb.variant.VariantMongoDBWriter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.HgvsMongo;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantAnnotation;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantAt;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantStatsMongo;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Mongo database representation of a Variant.
 */
@Document
public class VariantDocument {

    public static final String ONE_THOUSAND_STRING = VariantMongoDBWriter.CHUNK_SIZE_SMALL / 1000 + "k";

    public static final String TEN_THOUSAND_STRING = VariantMongoDBWriter.CHUNK_SIZE_BIG / 1000 + "k";

    public final static String TYPE_FIELD = "type";

    public final static String CHROMOSOME_FIELD = "chr";

    public final static String START_FIELD = "start";

    public final static String END_FIELD = "end";

    public final static String LENGTH_FIELD = "len";

    public final static String REFERENCE_FIELD = "ref";

    public final static String ALTERNATE_FIELD = "alt";

    public static final String AT_FIELD = "_at";

    public final static String HGVS_FIELD = "hgvs";

    public final static String IDS_FIELD = "ids";

    public final static String FILES_FIELD = "files";

    public final static String STATS_FIELD = "st";

    public final static String ANNOTATION_FIELD = "annot";

    @Id
    private String id;

    @Field(TYPE_FIELD)
    private Variant.VariantType variantType;

    @Field(CHROMOSOME_FIELD)
    private String chromosome;

    @Field(START_FIELD)
    private int start;

    @Field(END_FIELD)
    private int end;

    @Field(LENGTH_FIELD)
    private int length;

    @Field(REFERENCE_FIELD)
    private String reference;

    @Field(ALTERNATE_FIELD)
    private String alternate;

    @Field(AT_FIELD)
    private VariantAt at;

    @Field(HGVS_FIELD)
    private Set<HgvsMongo> hgvs;

    @Field(IDS_FIELD)
    private Set<String> ids;

    @Field(FILES_FIELD)
    private Set<VariantSourceEntryMongo> variantSources;

    @Field(STATS_FIELD)
    private Set<VariantStatsMongo> variantStatsMongo;

    @Field(ANNOTATION_FIELD)
    private Set<VariantAnnotation> annotations;

    VariantDocument(){
        //Empty constructor for spring
    }

    public VariantDocument(Variant.VariantType variantType, String chromosome, int start, int end, int length,
                           String reference, String alternate, Map<String, Set<String>> hgvs, Set<String> ids,
                           Set<VariantSourceEntryMongo> variantSources) {
        this.id = buildVariantId(chromosome, start, reference, alternate);
        this.variantType = variantType;
        this.chromosome = chromosome;
        this.start = start;
        this.end = end;
        this.length = length;
        this.reference = reference;
        this.alternate = alternate;
        this.at = generateAtField(chromosome, start);
        if (hgvs != null) {
            this.hgvs = createHgvsMongo(hgvs);
        }
        if (ids != null) {
            this.ids = new HashSet<>(ids);
        }
        if (variantSources != null) {
            this.variantSources = new HashSet<>(variantSources);
        }
    }

    public VariantDocument(Variant.VariantType variantType, String chromosome, int start, int end, int length,
                           String reference, String alternate, Set<HgvsMongo> hgvs, Set<String> ids,
                           Set<VariantSourceEntryMongo> variantSources) {
        this.id = buildVariantId(chromosome, start, reference, alternate);
        this.variantType = variantType;
        this.chromosome = chromosome;
        this.start = start;
        this.end = end;
        this.length = length;
        this.reference = reference;
        this.alternate = alternate;
        this.at = generateAtField(chromosome, start);
        if (hgvs != null && !hgvs.isEmpty()) {
            this.hgvs = new HashSet<>(hgvs);
        }
        if (ids != null && !ids.isEmpty()) {
            this.ids = new HashSet<>(ids);
        }
        if (variantSources != null && !variantSources.isEmpty()) {
            this.variantSources = new HashSet<>(variantSources);
        }
    }

    public static String buildVariantId(String chromosome, int start, String reference, String alternate) {
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

    public static VariantAt generateAtField(String chromosome, int start) {
        int smallChunkId = start / VariantMongoDBWriter.CHUNK_SIZE_SMALL;
        int bigChunkId = start / VariantMongoDBWriter.CHUNK_SIZE_BIG;
        String chunkSmall = chromosome + "_" + smallChunkId + "_" + ONE_THOUSAND_STRING;
        String chunkBig = chromosome + "_" + bigChunkId + "_" + TEN_THOUSAND_STRING;

        return new VariantAt(chunkSmall, chunkBig);
    }

    public static Set<HgvsMongo> createHgvsMongo(Map<String, Set<String>> hgvs) {
        Set<HgvsMongo> hgvsMongo = new HashSet<>();
        for (Map.Entry<String, Set<String>> entry : hgvs.entrySet()) {
            for (String value : entry.getValue()) {
                hgvsMongo.add(new HgvsMongo(entry.getKey(), value));
            }
        }
        return hgvsMongo;

    }

    public String getId() {
        return id;
    }

    public Variant.VariantType getVariantType() {
        return variantType;
    }

    public String getChromosome() {
        return chromosome;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
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

    public VariantAt getAt() {
        return at;
    }

    public Set<HgvsMongo> getHgvs() {
        return hgvs;
    }

    public Set<String> getIds() {
        return ids;
    }

    public Set<VariantSourceEntryMongo> getVariantSources() {
        return variantSources;
    }

    public Set<VariantStatsMongo> getVariantStatsMongo() {
        return variantStatsMongo;
    }

    public Set<VariantAnnotation> getAnnotations() {
        return annotations;
    }
}
