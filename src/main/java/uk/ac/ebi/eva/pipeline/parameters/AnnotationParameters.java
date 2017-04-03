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

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import uk.ac.ebi.eva.utils.URLHelper;

/**
 * Service that holds access to the values for annotatation steps like VEP etc.
 *
 * NOTE the @StepScope this is probably because the Step/Tasklet in this case the
 * {@link uk.ac.ebi.eva.pipeline.jobs.steps.tasklets.VepAnnotationGeneratorStep} is executed in parallel with statistics
 * {@link uk.ac.ebi.eva.pipeline.jobs.flows.PopulationStatisticsFlow} and they are not sharing the same context.
 * With @JobScope will not work!
 */
@Service
@StepScope
public class AnnotationParameters {
    private static final String PARAMETER = "#{jobParameters['";
    private static final String END = "']}";
    private static final String OR_EMPTY = "']?:''}";

    @Value(PARAMETER + JobParametersNames.OUTPUT_DIR_ANNOTATION + END)
    private String outputDirAnnotation;

    @Value(PARAMETER + JobParametersNames.INPUT_STUDY_ID + OR_EMPTY)
    private String studyId;

    @Value(PARAMETER + JobParametersNames.INPUT_VCF_ID + OR_EMPTY)
    private String fileId;

    @Value(PARAMETER + JobParametersNames.APP_VEP_PATH + END)
    private String vepPath;

    @Value(PARAMETER + JobParametersNames.APP_VEP_VERSION + END)
    private String vepVersion;

    @Value(PARAMETER + JobParametersNames.APP_VEP_CACHE_VERSION + END)
    private String vepCacheVersion;

    @Value(PARAMETER + JobParametersNames.APP_VEP_CACHE_PATH + END)
    private String vepCachePath;

    @Value(PARAMETER + JobParametersNames.APP_VEP_CACHE_SPECIES + END)
    private String vepCacheSpecies;

    @Value(PARAMETER + JobParametersNames.APP_VEP_NUMFORKS + END)
    private String vepNumForks;

    @Value(PARAMETER + JobParametersNames.INPUT_FASTA + END)
    private String inputFasta;

    public String getVepPath() {
        return vepPath;
    }

    public String getVepVersion() {
        return vepVersion;
    }

    public String getVepCacheVersion() {
        return vepCacheVersion;
    }

    public String getVepCachePath() {
        return vepCachePath;
    }

    public String getVepCacheSpecies() {
        return vepCacheSpecies;
    }

    public String getVepNumForks() {
        return vepNumForks;
    }

    public String getInputFasta() {
        return inputFasta;
    }

    public String getVepInput() {
        return URLHelper.resolveVepInput(outputDirAnnotation, studyId, fileId);
    }

    public String getVepOutput() {
        return URLHelper.resolveVepOutput(outputDirAnnotation, studyId, fileId);
    }

    public void setOutputDirAnnotation(String outputDirAnnotation) {
        this.outputDirAnnotation = outputDirAnnotation;
    }

    public void setStudyId(String studyId) {
        this.studyId = studyId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public void setVepPath(String vepPath) {
        this.vepPath = vepPath;
    }

    public void setVepCacheVersion(String vepCacheVersion) {
        this.vepCacheVersion = vepCacheVersion;
    }

    public void setVepCachePath(String vepCachePath) {
        this.vepCachePath = vepCachePath;
    }

    public void setVepCacheSpecies(String vepCacheSpecies) {
        this.vepCacheSpecies = vepCacheSpecies;
    }

    public void setVepNumForks(String vepNumForks) {
        this.vepNumForks = vepNumForks;
    }

    public void setInputFasta(String inputFasta) {
        this.inputFasta = inputFasta;
    }
}




