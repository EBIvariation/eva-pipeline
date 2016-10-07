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
package uk.ac.ebi.eva.pipeline.model;

import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Map;

/**
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class PopulationStats {

    @Field(value = "vid") private String variantId;
    @Field(value = "cid") private String cohortId;
    @Field(value = "sid") private String studyId;

    @Field(value = "maf") private double maf;
    @Field(value = "mgf") private double mgf;
    @Field(value = "mafAl") private String mafAllele;
    @Field(value = "mgfGt") private String mgfGenotype;
    @Field(value = "missAl") private int missingAlleles;
    @Field(value = "missGt") private int missingGenotypes;
    @Field(value = "numGt") private Map<String, Integer> genotypeCount;

    public String getVariantId() {
        return variantId;
    }

    public void setVariantId(String variantId) {
        this.variantId = variantId;
    }

    public String getCohortId() {
        return cohortId;
    }

    public void setCohortId(String cohortId) {
        this.cohortId = cohortId;
    }

    public String getStudyId() {
        return studyId;
    }

    public void setStudyId(String studyId) {
        this.studyId = studyId;
    }

    public double getMaf() {
        return maf;
    }

    public void setMaf(double maf) {
        this.maf = maf;
    }

    public double getMgf() {
        return mgf;
    }

    public void setMgf(double mgf) {
        this.mgf = mgf;
    }

    public String getMafAllele() {
        return mafAllele;
    }

    public void setMafAllele(String mafAllele) {
        this.mafAllele = mafAllele;
    }

    public String getMgfGenotype() {
        return mgfGenotype;
    }

    public void setMgfGenotype(String mgfGenotype) {
        this.mgfGenotype = mgfGenotype;
    }

    public int getMissingAlleles() {
        return missingAlleles;
    }

    public void setMissingAlleles(int missingAlleles) {
        this.missingAlleles = missingAlleles;
    }

    public int getMissingGenotypes() {
        return missingGenotypes;
    }

    public void setMissingGenotypes(int missingGenotypes) {
        this.missingGenotypes = missingGenotypes;
    }

    public Map<String, Integer> getGenotypeCount() {
        return genotypeCount;
    }

    public void setGenotypeCount(Map<String, Integer> genotypeCount) {
        this.genotypeCount = genotypeCount;
    }
}
