/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.utils;

import org.springframework.util.Assert;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder to generate the command line array that is accepted by the EvaPipelineCommandLineRunner.
 */
public class EvaCommandLineBuilder {

    private final Map<String, String> parameterMap;

    public EvaCommandLineBuilder() {
        this.parameterMap = new LinkedHashMap<>();
    }

    protected EvaCommandLineBuilder addString(String key, String parameter) {
        Assert.notNull(parameter);
        parameterMap.put(key, new String(parameter));
        return this;
    }

    private EvaCommandLineBuilder addBoolean(String key, boolean value) {
        parameterMap.put(key, Boolean.toString(value));
        return this;
    }

    public EvaCommandLineBuilder inputStudyId(String inputStudyId) {
        return addString(JobParametersNames.INPUT_STUDY_ID, inputStudyId);
    }

    public EvaCommandLineBuilder inputVcfId(String inputVcfId) {
        return addString(JobParametersNames.INPUT_VCF_ID, inputVcfId);
    }

    public EvaCommandLineBuilder inputVcf(String inputVcf) {
        return addString(JobParametersNames.INPUT_VCF, inputVcf);
    }

    public EvaCommandLineBuilder inputVcfAggregation(String inputVcfAggregation) {
        return addString(JobParametersNames.INPUT_VCF_AGGREGATION, inputVcfAggregation);
    }

    public EvaCommandLineBuilder databaseName(String databaseName) {
        return addString(JobParametersNames.DB_NAME, databaseName);
    }

    public EvaCommandLineBuilder vepPath(String vepPath) {
        return addString(JobParametersNames.APP_VEP_PATH, vepPath);
    }

    public EvaCommandLineBuilder vepCacheVersion(String vepCacheVersion) {
        return addString(JobParametersNames.APP_VEP_CACHE_VERSION, vepCacheVersion);
    }

    public EvaCommandLineBuilder vepVersion(String vepVersion) {
        return addString(JobParametersNames.APP_VEP_VERSION, vepVersion);
    }

    public EvaCommandLineBuilder vepCachePath(String vepCachePath) {
        return addString(JobParametersNames.APP_VEP_CACHE_PATH, vepCachePath);
    }

    public EvaCommandLineBuilder vepCacheSpecies(String vepCacheSpecies) {
        return addString(JobParametersNames.APP_VEP_CACHE_SPECIES, vepCacheSpecies);
    }

    public EvaCommandLineBuilder vepNumForks(String vepNumForks) {
        return addString(JobParametersNames.APP_VEP_NUMFORKS, vepNumForks);
    }

    public EvaCommandLineBuilder inputFasta(String inputFasta) {
        return addString(JobParametersNames.INPUT_FASTA, inputFasta);
    }

    public EvaCommandLineBuilder outputDirAnnotation(String outputDirAnnotation) {
        return addString(JobParametersNames.OUTPUT_DIR_ANNOTATION, outputDirAnnotation);
    }

    public EvaCommandLineBuilder outputDirStatistics(String outputDirStats) {
        return addString(JobParametersNames.OUTPUT_DIR_STATISTICS, outputDirStats);
    }

    public EvaCommandLineBuilder annotationSkip(boolean annotationSkip) {
        return addBoolean(JobParametersNames.ANNOTATION_SKIP, annotationSkip);
    }

    public EvaCommandLineBuilder annotationOverwrite(String annotationOverwrite) {
        return addString(JobParametersNames.ANNOTATION_OVERWRITE, annotationOverwrite);
    }

    public EvaCommandLineBuilder statisticsSkip(boolean statisticsSkip) {
        return addBoolean(JobParametersNames.STATISTICS_SKIP, statisticsSkip);
    }

    public String[] build() {
        List<String> parameters = new ArrayList<>();
        parameterMap.forEach((key, value) -> parameters.add("--" + key + "=" + value));
        return parameters.toArray(new String[parameters.size()]);
    }

    public EvaCommandLineBuilder inputStudyName(String studyName) {
        return addString(JobParametersNames.INPUT_STUDY_NAME, studyName);
    }

    public EvaCommandLineBuilder inputStudyType(String studyType) {
        return addString(JobParametersNames.INPUT_STUDY_TYPE, studyType);
    }

    public EvaCommandLineBuilder appVepPath(String appVepPath) {
        return addString(JobParametersNames.APP_VEP_PATH, appVepPath);
    }

    public EvaCommandLineBuilder appVepNumForks(String appVepNumForks) {
        return addString(JobParametersNames.APP_VEP_NUMFORKS, appVepNumForks);
    }

    public EvaCommandLineBuilder appVepCachePath(String appVepCachePath) {
        return addString(JobParametersNames.APP_VEP_CACHE_PATH, appVepCachePath);
    }

    public EvaCommandLineBuilder appVepCacheVersion(String appVepCacheVersion) {
        return addString(JobParametersNames.APP_VEP_CACHE_VERSION, appVepCacheVersion);
    }

    public EvaCommandLineBuilder appVepCacheSpecies(String appVepCacheSpecies) {
        return addString(JobParametersNames.APP_VEP_CACHE_SPECIES, appVepCacheSpecies);
    }

    public EvaCommandLineBuilder appVepTimeout(String appVepTimeout) {
        return addString(JobParametersNames.APP_VEP_TIMEOUT, appVepTimeout);
    }

    public EvaCommandLineBuilder configDbReadPreference(String preference) {
        return addString(JobParametersNames.CONFIG_DB_READPREFERENCE, preference);
    }

    public EvaCommandLineBuilder dbCollectionsVariantsName(String name) {
        return addString(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME, name);
    }

    public EvaCommandLineBuilder dbCollectionsFeaturesName(String name) {
        return addString(JobParametersNames.DB_COLLECTIONS_FEATURES_NAME, name);
    }

    public EvaCommandLineBuilder dbCollectionsFilesName(String name) {
        return addString(JobParametersNames.DB_COLLECTIONS_FILES_NAME, name);
    }

    public EvaCommandLineBuilder dbCollectionsStatisticsName(String name) {
        return addString(JobParametersNames.DB_COLLECTIONS_STATISTICS_NAME, name);
    }

    public EvaCommandLineBuilder dbCollectionsAnnotationMetadataName(String name) {
        return addString(JobParametersNames.DB_COLLECTIONS_ANNOTATION_METADATA_NAME, name);
    }

    public EvaCommandLineBuilder chunksize(String chunksize) {
        return addString(JobParametersNames.CONFIG_CHUNK_SIZE, chunksize);
    }

    public EvaCommandLineBuilder dbCollectionsAnnotationsName(String name) {
        return addString(JobParametersNames.DB_COLLECTIONS_ANNOTATIONS_NAME, name);
    }
}
