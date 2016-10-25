/*
 * Copyright 2015 EMBL - European Bioinformatics Institute
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

import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.utils.URLHelper;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 *
 * Tasklet that loads transformed variants into mongoDB
 *
 * Input: transformed variants file (variants.json.gz)
 * Output: variants loaded into mongodb
 */
@Component
@Import({JobOptions.class})
public class VariantLoaderStep implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(VariantLoaderStep.class);

    @Autowired
    private JobOptions jobOptions;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        Map<String, Object> jobParameters = chunkContext.getStepContext().getJobParameters();

        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        URI outdirUri = URLHelper.createUri(String.valueOf(jobParameters.get("output.dir")));
        URI inputFileUri = URLHelper.createUri(String.valueOf(jobParameters.get("input.vcf")));

        Path input = Paths.get(inputFileUri.getPath());
        Path output = Paths.get(outdirUri.getPath());
        Path outputVariantJsonFile = output.resolve(input.getFileName().toString() + ".variants.json" + jobOptions.getCompressExtension());
        URI transformedVariantsUri = outdirUri.resolve(outputVariantJsonFile.getFileName().toString());

        logger.info("Loading variants from file {}", inputFileUri);
        variantStorageManager.load(transformedVariantsUri, jobOptions.getVariantOptions());

        return RepeatStatus.FINISHED;
    }

}
