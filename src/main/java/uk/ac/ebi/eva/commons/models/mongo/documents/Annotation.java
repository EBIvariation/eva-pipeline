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
package uk.ac.ebi.eva.commons.models.mongo.documents;

import com.google.common.base.Strings;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments.ConsequenceType;
import uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments.Xref;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Slim version of {@link org.opencb.biodata.models.variant.annotation.VariantAnnotation}
 * Unused fields removed.
 */
@Document
public class Annotation {

    public static final String CHROMOSOME_FIELD = "chr";

    public static final String START_FIELD = "start";

    public static final String END_FIELD = "end";

    public static final String VEP_VERSION_FIELD = "vepv";

    public static final String VEP_CACHE_VERSION_FIELD = "cachev";

    public static final String CONSEQUENCE_TYPE_FIELD = "ct";

    public static final String XREFS_FIELD = "xrefs";

    @Id
    private String id;

    @Field(value = CHROMOSOME_FIELD)
    private String chromosome;

    @Field(value = START_FIELD)
    private int start;

    @Field(value = END_FIELD)
    private int end;

    @Field(value = VEP_VERSION_FIELD)
    private String vepVersion;

    @Field(value = VEP_CACHE_VERSION_FIELD)
    private String vepCacheVersion;

    @Field(value = CONSEQUENCE_TYPE_FIELD)
    private Set<ConsequenceType> consequenceTypes;

    @Field(value = XREFS_FIELD)
    private Set<Xref> xrefs;

    Annotation() {
        // Empty document constructor for spring-data
    }

    public Annotation(String chromosome, int start, int end, String referenceAllele, String alternativeAllele,
                      String vepVersion, String vepCacheVersion) {
        this.chromosome = chromosome;
        this.start = start;
        this.end = end;
        this.vepVersion = vepVersion;
        this.vepCacheVersion = vepCacheVersion;

        this.id = buildAnnotationId(chromosome, start, referenceAllele, alternativeAllele, vepVersion, vepCacheVersion);
        this.xrefs = new HashSet<>();
        this.consequenceTypes = new HashSet<>();
    }

    /**
     * Private copy constructor
     *
     * @param annotation
     */
    private Annotation(Annotation annotation) {
        chromosome = annotation.chromosome;
        start = annotation.start;
        end = annotation.end;
        vepVersion = annotation.vepVersion;
        vepCacheVersion = annotation.vepCacheVersion;

        id = annotation.id;
        xrefs = new HashSet<>();
        consequenceTypes = new HashSet<>();

        xrefs.addAll(annotation.xrefs);
        consequenceTypes.addAll(annotation.consequenceTypes);
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

    public String getId() {
        return id;
    }

    public Set<Xref> getXrefs() {
        return xrefs;
    }

    public Set<ConsequenceType> getConsequenceTypes() {
        return Collections.unmodifiableSet(consequenceTypes);
    }

    public void addConsequenceType(ConsequenceType consequenceType) {
        consequenceTypes.add(consequenceType);
        generateXrefsFromConsequenceType(consequenceType);
    }

    public void addConsequenceTypes(Set<ConsequenceType> consequenceTypes) {
        for(ConsequenceType consequenceType: consequenceTypes){
            addConsequenceType(consequenceType);
        }
    }

    public String getVepVersion() {
        return vepVersion;
    }

    public String getVepCacheVersion() {
        return vepCacheVersion;
    }

    private void generateXrefsFromConsequenceType(ConsequenceType consequenceType) {
        if (!Strings.isNullOrEmpty(consequenceType.getGeneName())) {
            xrefs.add(new Xref(consequenceType.getGeneName(), "HGNC"));
        }
        if (!Strings.isNullOrEmpty(consequenceType.getEnsemblGeneId())) {
            xrefs.add(new Xref(consequenceType.getEnsemblGeneId(), "ensemblGene"));
        }
        if (!Strings.isNullOrEmpty(consequenceType.getEnsemblTranscriptId())) {
            xrefs.add(new Xref(consequenceType.getEnsemblTranscriptId(), "ensemblTranscript"));
        }
    }

    public static String buildAnnotationId(String chromosome, int start, String reference, String alternate,
                                           String vepVersion, String vepCacheVersion) {
        StringBuilder builder = new StringBuilder(Variant.buildVariantId(chromosome, start, reference, alternate));
        builder.append("_");
        builder.append(vepVersion);
        builder.append("_");
        builder.append(vepCacheVersion);
        return builder.toString();
    }

    /**
     * Builds the variant id from the current annotation id. In essence we remove the two extra fields added at the end
     * of the id and the underscores.
     * @return
     */
    public String buildVariantId() {
        return getId().substring(0, getId().length() - vepVersion.length() -vepCacheVersion.length() -2);
    }

    /**
     * Concatenate two annotations in a new one. This method returns a new instance with the concatenated array of
     * consequence types and computed xrefs.
     *
     * @param annotation
     * @return
     */
    public Annotation concatenate(Annotation annotation) {
        Annotation temp = new Annotation(this);
        if(annotation.getConsequenceTypes()!=null){
            temp.addConsequenceTypes(annotation.getConsequenceTypes());
        }
        return temp;
    }
}
