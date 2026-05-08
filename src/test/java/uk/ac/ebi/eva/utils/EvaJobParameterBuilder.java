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

import org.springframework.batch.core.JobParametersBuilder;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import java.time.LocalDateTime;

public class EvaJobParameterBuilder extends JobParametersBuilder {

    public EvaJobParameterBuilder annotationOverwrite(String outputDirAnnotation) {
        addString(JobParametersNames.ANNOTATION_OVERWRITE, outputDirAnnotation);
        return this;
    }

    public EvaJobParameterBuilder inputStudyId(String inputStudyId) {
        addString(JobParametersNames.INPUT_STUDY_ID, inputStudyId);
        return this;
    }

    public EvaJobParameterBuilder inputStudyType(String inputStudyType) {
        addString(JobParametersNames.INPUT_STUDY_TYPE, inputStudyType);
        return this;
    }

    public EvaJobParameterBuilder inputStudyName(String inputStudyName) {
        addString(JobParametersNames.INPUT_STUDY_NAME, inputStudyName);
        return this;
    }

    public EvaJobParameterBuilder inputVcfId(String inputVcfId) {
        addString(JobParametersNames.INPUT_VCF_ID, inputVcfId);
        return this;
    }

    public EvaJobParameterBuilder inputVcf(String inputVcf) {
        addString(JobParametersNames.INPUT_VCF, inputVcf);
        return this;
    }

    public EvaJobParameterBuilder inputVcfAggregation(String inputVcfAggregation) {
        addString(JobParametersNames.INPUT_VCF_AGGREGATION, inputVcfAggregation);
        return this;
    }

    public EvaJobParameterBuilder inputAccessionReport(String inputAccessionReport) {
        addString(JobParametersNames.INPUT_ACCESSION_REPORT, inputAccessionReport);
        return this;
    }

    public EvaJobParameterBuilder inputAssemblyReport(String inputAssemblyReport) {
        addString(JobParametersNames.INPUT_ASSEMBLY_REPORT, inputAssemblyReport);
        return this;
    }

    public EvaJobParameterBuilder timestamp() {
        addLocalDateTime("timestamp", LocalDateTime.now(), false);
        return this;
    }

    public EvaJobParameterBuilder databaseName(String databaseName) {
        addString(JobParametersNames.DB_NAME, databaseName);
        return this;
    }

    public EvaJobParameterBuilder collectionVariantsName(String collectionVariantsName) {
        addString(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME, collectionVariantsName);
        return this;
    }

    public EvaJobParameterBuilder collectionFilesName(String collectionFilesName) {
        addString(JobParametersNames.DB_COLLECTIONS_FILES_NAME, collectionFilesName);
        return this;
    }

    public EvaJobParameterBuilder collectionFeaturesName(String collectionFeaturesName) {
        addString(JobParametersNames.DB_COLLECTIONS_FEATURES_NAME, collectionFeaturesName);
        return this;
    }

    public EvaJobParameterBuilder collectionAnnotationMetadataName(String collectionAnnotationMetadataName) {
        addString(JobParametersNames.DB_COLLECTIONS_ANNOTATION_METADATA_NAME, collectionAnnotationMetadataName);
        return this;
    }

    public EvaJobParameterBuilder collectionAnnotationsName(String collectionAnnotationsName) {
        addString(JobParametersNames.DB_COLLECTIONS_ANNOTATIONS_NAME, collectionAnnotationsName);
        return this;
    }

    public EvaJobParameterBuilder vepPath(String vepPath) {
        addString(JobParametersNames.APP_VEP_PATH, vepPath);
        return this;
    }

    public EvaJobParameterBuilder vepVersion(String vepVersion) {
        addString(JobParametersNames.APP_VEP_VERSION, vepVersion);
        return this;
    }

    public EvaJobParameterBuilder vepCacheVersion(String vepCacheVersion) {
        addString(JobParametersNames.APP_VEP_CACHE_VERSION, vepCacheVersion);
        return this;
    }

    public EvaJobParameterBuilder vepCachePath(String vepCachePath) {
        addString(JobParametersNames.APP_VEP_CACHE_PATH, vepCachePath);
        return this;
    }

    public EvaJobParameterBuilder vepCacheSpecies(String vepCacheSpecies) {
        addString(JobParametersNames.APP_VEP_CACHE_SPECIES, vepCacheSpecies);
        return this;
    }

    public EvaJobParameterBuilder vepNumForks(String vepNumForks) {
        addString(JobParametersNames.APP_VEP_NUMFORKS, vepNumForks);
        return this;
    }

    public EvaJobParameterBuilder vepTimeout(String vepTimeout) {
        addString(JobParametersNames.APP_VEP_TIMEOUT, vepTimeout);
        return this;
    }

    public EvaJobParameterBuilder inputFasta(String inputFasta) {
        addString(JobParametersNames.INPUT_FASTA, inputFasta);
        return this;
    }

    public EvaJobParameterBuilder outputDirAnnotation(String outputDirAnnotation) {
        addString(JobParametersNames.OUTPUT_DIR_ANNOTATION, outputDirAnnotation);
        return this;
    }

    public EvaJobParameterBuilder outputDirStats(String outputDirStats) {
        addString(JobParametersNames.OUTPUT_DIR_STATISTICS, outputDirStats);
        return this;
    }

    public EvaJobParameterBuilder annotationSkip(boolean annotationSkip) {
        addString(JobParametersNames.ANNOTATION_SKIP, Boolean.toString(annotationSkip));
        return this;
    }

    public EvaJobParameterBuilder statisticsSkip(boolean statisticsSkip) {
        addString(JobParametersNames.STATISTICS_SKIP, Boolean.toString(statisticsSkip));
        return this;
    }

    public EvaJobParameterBuilder chunkSize(String chunkSize) {
        addString(JobParametersNames.CONFIG_CHUNK_SIZE, chunkSize);
        return this;
    }

    public EvaJobParameterBuilder inputGtf(String inputGtf) {
        addString(JobParametersNames.INPUT_GTF, inputGtf);
        return this;
    }
}
