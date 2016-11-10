/*
 * Copyright 2015-2016 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.jobs.steps;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import uk.ac.ebi.eva.pipeline.configuration.JobParametersNames;
import uk.ac.ebi.eva.utils.FileUtils;
import uk.ac.ebi.eva.utils.URLHelper;

import java.net.URI;

/**
 * Tasklet that normalizes variants. To see the applied rules please refer to:
 *
 * @see <a href="www.ebi.ac.uk/eva/?FAQ">www.ebi.ac.uk/eva/?FAQ</a>
 * @see <a href="https://docs.google.com/presentation/d/1WqSiT5AEEQF9jdIewdYIp-I0G5ozkFP3IikfCJZO1dc/edit#slide=id.ge1548f905_0_592">EVA FAQ</a>
 *
 * Input: vcf file
 * Output: transformed variants file (variants.json.gz)
 */
public class VariantNormalizerStep implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(VariantNormalizerStep.class);

    private final ObjectMap variantOptions;
    private final ObjectMap pipelineOptions;

    public VariantNormalizerStep(ObjectMap variantOptions, ObjectMap pipelineOptions) {
        this.variantOptions = variantOptions;
        this.pipelineOptions = pipelineOptions;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        String outputDirPath = pipelineOptions.getString(JobParametersNames.OUTPUT_DIR);
        String inputVcf = pipelineOptions.getString(JobParametersNames.INPUT_VCF);
        String inputPedigree = pipelineOptions.getString(JobParametersNames.INPUT_PEDIGREE);

        URI outdirUri = FileUtils.getPathUri(outputDirPath,true);
        URI nextFileUri = URLHelper.createUri(inputVcf);
        URI pedigreeUri =  inputPedigree!= null ? URLHelper.createUri(inputPedigree) : null;

        logger.info("Transform file {} to {}", inputVcf, outputDirPath);

        logger.info("Extract variants '{}'", nextFileUri);
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        variantStorageManager.extract(nextFileUri, outdirUri, variantOptions);

        logger.info("PreTransform variants '{}'", nextFileUri);
        variantStorageManager.preTransform(nextFileUri, variantOptions);
        logger.info("Transform variants '{}'", nextFileUri);
        variantStorageManager.transform(nextFileUri, pedigreeUri, outdirUri, variantOptions);
        logger.info("PostTransform variants '{}'", nextFileUri);
        variantStorageManager.postTransform(nextFileUri, variantOptions);
        return RepeatStatus.FINISHED;
    }

}
