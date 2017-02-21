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
package uk.ac.ebi.eva.commons.models.data;

import org.opencb.biodata.models.variant.annotation.ConsequenceType;
import org.opencb.biodata.models.variant.annotation.Xref;
import org.opencb.biodata.models.variation.PopulationFrequency;

import java.util.List;
import java.util.Map;


/**
 * Slim version of {@link org.opencb.biodata.models.variant.annotation.VariantAnnotation}
 * Unused fields removed.
 *
 */
public class VariantAnnotation {

    private String chromosome;

    private int start;

    private int end;

    private String referenceAllele;

    private String alternativeAllele;

    private String id;

    private List<Xref> xrefs;

    private List<String> hgvs;

    private List<ConsequenceType> consequenceTypes;

    private List<PopulationFrequency> populationFrequencies = null;

    private Map<String, Object> additionalAttributes;

    public VariantAnnotation() {
        this(null, 0, 0, null);
    }

    public VariantAnnotation(String chromosome, int start, int end, String referenceAllele) {
        this.chromosome = chromosome;
        this.start = start;
        this.end = end;
        this.referenceAllele = referenceAllele;
    }

    public VariantAnnotation(String chromosome, int start, int end, String referenceAllele, String alternativeAllele) {
        this.chromosome = chromosome;
        this.start = start;
        this.end = end;
        this.referenceAllele = referenceAllele;
        this.alternativeAllele = alternativeAllele;
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

    public List<Xref> getXrefs() {
        return xrefs;
    }

    public void setXrefs(List<Xref> xrefs) {
        this.xrefs = xrefs;
    }

    public List<String> getHgvs() {
        return hgvs;
    }

    public void setHgvs(List<String> hgvs) {
        this.hgvs = hgvs;
    }

    public List<ConsequenceType> getConsequenceTypes() {
        return consequenceTypes;
    }

    public void setConsequenceTypes(List<ConsequenceType> consequenceTypes) {
        this.consequenceTypes = consequenceTypes;
    }

    public List<PopulationFrequency> getPopulationFrequencies() {
        return populationFrequencies;
    }

    public void setPopulationFrequencies(List<PopulationFrequency> populationFrequencies) {
        this.populationFrequencies = populationFrequencies;
    }

}
