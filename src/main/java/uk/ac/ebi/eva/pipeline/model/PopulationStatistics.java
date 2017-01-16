/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
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

import org.springframework.context.annotation.Profile;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import uk.ac.ebi.eva.pipeline.Application;

import java.util.Map;

/**
 * Statistics related to a set of samples for a given variant.
 * <p>
 * Setters have package visibility, the user should use the constructor to instantiate valid objects.
 */
@Profile(Application.MONGO_EXPERIMENTAL_PROFILE)
@Document
@CompoundIndexes({
    @CompoundIndex(name = "vscid", def = "{'chr': 1, 'start': 1, 'ref': 1, 'alt': 1, 'sid': 1, 'cid': 1}", unique = true)
})
public class PopulationStatistics {

    @Field(value = "vid") private String variantId;
    @Field(value = "chr") private String chromosome;
    @Field(value = "start") private int start;
    @Field(value = "ref") private String reference;
    @Field(value = "alt") private String alternate;

    @Field(value = "cid") private String cohortId;
    @Field(value = "sid") private String studyId;

    @Field(value = "maf") private double maf;
    @Field(value = "mgf") private double mgf;
    @Field(value = "mafAl") private String mafAllele;
    @Field(value = "mgfGt") private String mgfGenotype;
    @Field(value = "missAl") private int missingAlleles;
    @Field(value = "missGt") private int missingGenotypes;
    @Field(value = "numGt") private Map<String, Integer> genotypeCount;

    @PersistenceConstructor
    public PopulationStatistics(String variantId,
                                String chromosome,
                                int start,
                                String reference,
                                String alternate,
                                String cohortId,
                                String studyId,
                                double maf,
                                double mgf,
                                String mafAllele,
                                String mgfGenotype,
                                int missingAlleles,
                                int missingGenotypes,
                                Map<String, Integer> genotypeCount) {

        setVariantId(variantId);
        setChromosome(chromosome);
        setStart(start);
        setReference(reference);
        setAlternate(alternate);

        setCohortId(cohortId);
        setStudyId(studyId);

        setMaf(maf);
        setMgf(mgf);
        setMafAllele(mafAllele);
        setMgfGenotype(mgfGenotype);

        setMissingAlleles(missingAlleles);
        setMissingGenotypes(missingGenotypes);
        setGenotypeCount(genotypeCount);

    }

    public String getVariantId() {
        return variantId;
    }

    void setVariantId(String variantId) {
        if (variantId == null || variantId.isEmpty()) {
            throw new IllegalArgumentException("variantId (vid) should not be null");
        }
        this.variantId = variantId;
    }
    public String getChromosome() {
        return chromosome;
    }

    void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public int getStart() {
        return start;
    }

    void setStart(int start) {
        this.start = start;
    }

    public String getReference() {
        return reference;
    }

    void setReference(String reference) {
        this.reference = reference;
    }

    public String getAlternate() {
        return alternate;
    }

    void setAlternate(String alternate) {
        this.alternate = alternate;
    }

    public String getCohortId() {
        return cohortId;
    }

    void setCohortId(String cohortId) {
        if (cohortId == null || cohortId.isEmpty()) {
            throw new IllegalArgumentException("cohortId (cid) should not be null");
        }
        this.cohortId = cohortId;
    }

    public String getStudyId() {
        return studyId;
    }

    void setStudyId(String studyId) {
        if (studyId == null || studyId.isEmpty()) {
            throw new IllegalArgumentException("studyId (sid) should not be null");
        }
        this.studyId = studyId;
    }

    public double getMaf() {
        return maf;
    }

    void setMaf(double maf) {
        if ((maf < 0.0 || maf > 1.0) && maf != -1.0) {
            throw new IllegalArgumentException("Minimum allele frequency (maf=" + maf + ") should be in range [0.0, 1.0] or -1.0 if undefined");
        }
        this.maf = maf;
    }

    public double getMgf() {
        return mgf;
    }

    void setMgf(double mgf) {
        if ((mgf < 0.0 || mgf > 1.0) && mgf != -1.0) {
            throw new IllegalArgumentException("Minimum genotype frequency (mgf=" + mgf + ") should be in range [0.0, 1.0] or -1.0 if undefined");
        }
        this.mgf = mgf;
    }

    public String getMafAllele() {
        return mafAllele;
    }

    void setMafAllele(String mafAllele) {
        this.mafAllele = mafAllele;
    }

    public String getMgfGenotype() {
        return mgfGenotype;
    }

    void setMgfGenotype(String mgfGenotype) {
        this.mgfGenotype = mgfGenotype;
    }

    public int getMissingAlleles() {
        return missingAlleles;
    }

    void setMissingAlleles(int missingAlleles) {
        if (missingAlleles < 0 && missingAlleles != -1) {
            throw new IllegalArgumentException("Missing alleles (missAl = " + missingAlleles + ") should be in range [0, inf) or -1 if undefined");
        }
        this.missingAlleles = missingAlleles;
    }

    public int getMissingGenotypes() {
        return missingGenotypes;
    }

    void setMissingGenotypes(int missingGenotypes) {
        if (missingGenotypes < 0 && missingGenotypes != -1) {
            throw new IllegalArgumentException("Missing genotypes (missGt = " + missingGenotypes + ") should be in range [0, inf) or -1 if undefined");
        }
        this.missingGenotypes = missingGenotypes;
    }

    public Map<String, Integer> getGenotypeCount() {
        return genotypeCount;
    }

    void setGenotypeCount(Map<String, Integer> genotypeCount) {
        for (Map.Entry<String, Integer> entry : genotypeCount.entrySet()) {
            if (entry.getValue() < 0) {
                throw new IllegalArgumentException("Genotype count (numGT[" + entry.getKey() + "] = "
                        + entry.getValue() + ") should be in range [0, inf) or -1 if undefined");
            }
        }
        this.genotypeCount = genotypeCount;
    }
}
