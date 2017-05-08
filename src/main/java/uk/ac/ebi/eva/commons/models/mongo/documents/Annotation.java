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
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import uk.ac.ebi.eva.commons.models.data.AnnotationFieldNames;
import uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments.ConsequenceType;
import uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments.Xref;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Slim version of {@link org.opencb.biodata.models.variant.annotation.VariantAnnotation}
 * Unused fields removed.
 */
@Document
public class Annotation {

    @Id
    private String id;

    @Field(value = AnnotationFieldNames.CHROMOSOME_FIELD)
    private String chromosome;

    @Field(value = AnnotationFieldNames.START_FIELD)
    private int start;

    @Field(value = AnnotationFieldNames.END_FIELD)
    private int end;

    @Transient
    private String referenceAllele;

    @Transient
    private String alternativeAllele;

    @Field(value = AnnotationFieldNames.ENSEMBL_VERSION_FIELD)
    private String ensemblVersion;

    @Field(value = AnnotationFieldNames.VEP_CACHE_VERSION_FIELD)
    private String vepCacheVersion;

    @Field(value = AnnotationFieldNames.XREFS_FIELD)
    private Set<Xref> xrefs;

    @Field(value = AnnotationFieldNames.CONSEQUENCE_TYPE_FIELD)
    private Set<ConsequenceType> consequenceTypes;

    @Transient
    private Map<String, Object> additionalAttributes;

    Annotation(){
        // Empty document constructor for spring-data
    }

    public Annotation(String chromosome, int start, int end, String referenceAllele) {
        this(chromosome, start, end, referenceAllele, "");
    }

    public Annotation(String chromosome, int start, int end, String referenceAllele, String alternativeAllele) {
        this.chromosome = chromosome;
        this.start = start;
        this.end = end;
        this.referenceAllele = referenceAllele;
        this.alternativeAllele = alternativeAllele;

        this.id = "";
        this.xrefs = new HashSet<>();
        this.consequenceTypes = new HashSet<>();
        this.additionalAttributes = new HashMap<>();
    }

    public String getChromosome() {
        return chromosome;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public String getReferenceAllele() {
        return referenceAllele;
    }

    public String getAlternativeAllele() {
        return alternativeAllele;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Set<Xref> getXrefs() {
        return xrefs;
    }

    public void setXrefs(Set<Xref> xrefs) {
        this.xrefs = xrefs;
    }

    public Set<ConsequenceType> getConsequenceTypes() {
        return consequenceTypes;
    }

    public void setConsequenceTypes(Set<ConsequenceType> consequenceTypes) {
        this.consequenceTypes = consequenceTypes;
    }

    public String getEnsemblVersion() {
        return ensemblVersion;
    }

    public void setEnsemblVersion(String ensemblVersion) {
        this.ensemblVersion = ensemblVersion;
    }

    public String getVepCacheVersion() {
        return vepCacheVersion;
    }

    public void setVepCacheVersion(String vepCacheVersion) {
        this.vepCacheVersion = vepCacheVersion;
    }

    public void generateXrefsFromConsequenceTypes() {
        for (ConsequenceType consequenceType : consequenceTypes) {
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
    }

}
