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

import com.sun.mail.imap.protocol.ENVELOPE;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParametersBuilder;

import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import java.sql.Timestamp;
import java.util.Date;

public class EvaJobParameterBuilder extends JobParametersBuilder {

    public EvaJobParameterBuilder inputStudyId(String inputStudyId) {
        addParameter(JobParametersNames.INPUT_STUDY_ID, new JobParameter(inputStudyId));
        return this;
    }

    public EvaJobParameterBuilder inputVcfId(String inputVcfId) {
        addParameter(JobParametersNames.INPUT_VCF_ID, new JobParameter(inputVcfId));
        return this;
    }

    public EvaJobParameterBuilder inputVcf(String inputVcf) {
        addParameter(JobParametersNames.INPUT_VCF, new JobParameter(inputVcf));
        return this;
    }

    public EvaJobParameterBuilder inputVcfAggregation(String inputVcfAggregation) {
        addParameter(JobParametersNames.INPUT_VCF_AGGREGATION, new JobParameter(inputVcfAggregation));
        return this;
    }

    public EvaJobParameterBuilder timestamp() {
        addParameter("timestamp", new JobParameter(new Timestamp(new Date().getTime())));
        return this;
    }

    public EvaJobParameterBuilder databaseName(String databaseName) {
        addParameter(JobParametersNames.DB_NAME, new JobParameter(databaseName));
        return this;
    }

    public EvaJobParameterBuilder collectionVariantsName(String collectionVariantsName) {
        addParameter(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME, new JobParameter(collectionVariantsName));
        return this;
    }
    public EvaJobParameterBuilder collectionFilesName(String collectionFilesName) {
        addParameter(JobParametersNames.DB_COLLECTIONS_FILES_NAME, new JobParameter(collectionFilesName));
        return this;
    }

    public EvaJobParameterBuilder collectionFeaturesName(String collectionFeaturesName) {
        addParameter(JobParametersNames.DB_COLLECTIONS_FEATURES_NAME, new JobParameter(collectionFeaturesName));
        return this;
    }

    public EvaJobParameterBuilder vepPath(String vepPath) {
        addParameter(JobParametersNames.APP_VEP_PATH, new JobParameter(vepPath));
        return this;
    }

    public EvaJobParameterBuilder vepCacheVersion(String vepCacheVersion) {
        addParameter(JobParametersNames.APP_VEP_CACHE_VERSION, new JobParameter(vepCacheVersion));
        return this;
    }

    public EvaJobParameterBuilder vepCachePath(String vepCachePath) {
        addParameter(JobParametersNames.APP_VEP_CACHE_PATH, new JobParameter(vepCachePath));
        return this;
    }

    public EvaJobParameterBuilder vepCacheSpecies(String vepCacheSpecies) {
        addParameter(JobParametersNames.APP_VEP_CACHE_SPECIES, new JobParameter(vepCacheSpecies));
        return this;
    }

    public EvaJobParameterBuilder vepNumForks(String vepNumForks) {
        addParameter(JobParametersNames.APP_VEP_NUMFORKS, new JobParameter(vepNumForks));
        return this;
    }

    public EvaJobParameterBuilder inputFasta(String inputFasta) {
        addParameter(JobParametersNames.INPUT_FASTA, new JobParameter(inputFasta));
        return this;
    }

    public EvaJobParameterBuilder outputDirAnnotation(String outputDirAnnotation) {
        addParameter(JobParametersNames.OUTPUT_DIR_ANNOTATION, new JobParameter(outputDirAnnotation));
        return this;
    }

    public EvaJobParameterBuilder outputDirStats(String outputDirStats) {
        addParameter(JobParametersNames.OUTPUT_DIR_STATISTICS, new JobParameter(outputDirStats));
        return this;
    }
}
