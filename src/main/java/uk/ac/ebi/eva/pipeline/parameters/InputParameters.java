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
package uk.ac.ebi.eva.pipeline.parameters;

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Service that holds access to Job input parameters.
 */
@Service
@StepScope
public class InputParameters {

    private static final String PARAMETER = "#{jobParameters['";
    private static final String END = "']}";
    private static final String OR_NULL = "']?:null}";
    private static final String OR_EMPTY = "']?:''}";

    @Value(PARAMETER + JobParametersNames.INPUT_STUDY_ID + END)
    private String studyId;

    @Value(PARAMETER + JobParametersNames.INPUT_VCF_ID + OR_EMPTY)
    private String vcfId;

    @Value(PARAMETER + JobParametersNames.INPUT_VCF + END)
    private String vcf;

    @Value(PARAMETER + JobParametersNames.INPUT_GTF + END)
    private String gtf;

    @Value(PARAMETER + JobParametersNames.INPUT_PEDIGREE + END)
    private String pedigree;

    @Value(PARAMETER + JobParametersNames.INPUT_VCF_AGGREGATION + "']?:'NONE'}")
    private String vcfAggregation;

    @Value(PARAMETER + JobParametersNames.INPUT_STUDY_NAME + END)
    private String studyName;

    @Value(PARAMETER + JobParametersNames.INPUT_STUDY_TYPE + END)
    private VariantStudy.StudyType studyType;

    @Value(PARAMETER + JobParametersNames.INPUT_VCF_AGGREGATION_MAPPING_PATH + OR_NULL)
    private String aggregatedMappingFile;

    @Value(PARAMETER + JobParametersNames.CONFIG_CHUNK_SIZE + "']?:1000}")
    private int chunkSize;

    public String getVcf() {
        return vcf;
    }

    public VariantSource.Aggregation getVcfAggregation() {
        return VariantSource.Aggregation.valueOf(vcfAggregation);
    }

    public String getStudyId() {
        return studyId;
    }

    public String getVcfId() {
        return vcfId;
    }

    public String getStudyName() {
        return studyName;
    }

    public VariantStudy.StudyType getStudyType() {
        return studyType;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public String getGtf() {
        return gtf;
    }

    public String getPedigree() {
        return pedigree;
    }

    public String getAggregatedMappingFile() {
        return aggregatedMappingFile;
    }
}
