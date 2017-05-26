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
package uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments;

import org.opencb.biodata.models.feature.Genotype;
import org.springframework.data.mongodb.core.mapping.Field;
import uk.ac.ebi.eva.commons.models.data.VariantStats;

import java.util.HashMap;
import java.util.Map;

/**
 * Mongo database representation of Variant Stats.
 */
public class VariantStatsMongo {

    public final static String COHORT_ID = "cid";

    public final static String STUDY_ID = "sid";

    public final static String FILE_ID = "fid";

    public final static String MAF_FIELD = "maf";

    public final static String MGF_FIELD = "mgf";

    public final static String MAFALLELE_FIELD = "mafAl";

    public final static String MGFGENOTYPE_FIELD = "mgfGt";

    public final static String MISSALLELE_FIELD = "missAl";

    public final static String MISSGENOTYPE_FIELD = "missGt";

    public final static String NUMGT_FIELD = "numGt";

    @Field(STUDY_ID)
    private String studyId;

    @Field(FILE_ID)
    private String fileId;

    @Field(COHORT_ID)
    private String cohortId;

    @Field(MAF_FIELD)
    private float maf;

    @Field(MGF_FIELD)
    private float mgf;

    @Field(MAFALLELE_FIELD)
    private String mafAllele;

    @Field(MGFGENOTYPE_FIELD)
    private String mgfGenotype;

    @Field(MISSALLELE_FIELD)
    private int missingAlleles;

    @Field(MISSGENOTYPE_FIELD)
    private int missingGenotypes;

    @Field(NUMGT_FIELD)
    private Map<String, Integer> numGt;

    VariantStatsMongo(){
        //Empty constructor for spring
    }

    public VariantStatsMongo(String studyId, String fileId, String cohortId, VariantStats stats) {
        this.studyId = studyId;
        this.fileId = fileId;
        this.cohortId = cohortId;
        this.maf = stats.getMaf();
        this.mgf = stats.getMgf();
        this.mafAllele = stats.getMafAllele();
        this.mgfGenotype = stats.getMgfGenotype();
        this.missingAlleles = stats.getMissingAlleles();
        this.missingGenotypes = stats.getMissingGenotypes();
        this.numGt = buildGenotypes(stats.getGenotypesCount());

    }

    private Map<String, Integer> buildGenotypes(Map<Genotype, Integer> genotypesCount) {
        Map<String,Integer> genotypes = new HashMap<>();
        for (Map.Entry<Genotype, Integer> g : genotypesCount.entrySet()) {
            String genotypeStr = g.getKey().toString().replace(".", "-1");
            genotypes.put(genotypeStr, g.getValue());
        }
        return genotypes;
    }

    public String getStudyId() {
        return studyId;
    }

    public String getFileId() {
        return fileId;
    }

    public String getCohortId() {
        return cohortId;
    }

    public float getMaf() {
        return maf;
    }

    public float getMgf() {
        return mgf;
    }

    public String getMafAllele() {
        return mafAllele;
    }

    public String getMgfGenotype() {
        return mgfGenotype;
    }

    public int getMissingAlleles() {
        return missingAlleles;
    }

    public int getMissingGenotypes() {
        return missingGenotypes;
    }

    public Map<String, Integer> getNumGt() {
        return numGt;
    }
}
