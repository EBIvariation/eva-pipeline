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
package embl.ebi.variation.eva.pipeline.steps;

import embl.ebi.variation.eva.pipeline.jobs.VariantConfiguration;
import embl.ebi.variation.eva.pipeline.listeners.JobParametersListener;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

/**
 * Created by jmmut on 2015-11-10.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class VariantsTransform implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(VariantsTransform.class);
    private JobParametersListener listener;

    public VariantsTransform(JobParametersListener listener) {
        this.listener = listener;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        JobParameters parameters = chunkContext.getStepContext().getStepExecution().getJobParameters();
        ObjectMap variantOptions = listener.getVariantOptions();

        URI outdirUri = createUri(parameters.getString("outputDir"));
        URI nextFileUri = createUri(parameters.getString("input"));
        URI pedigreeUri = parameters.getString("pedigree") != null ? createUri(parameters.getString("pedigree")) : null;

        logger.info("transform file " + parameters.getString("input") + " to " + parameters.getString("outputDir"));

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

    public static URI createUri(String input) throws URISyntaxException {
        URI sourceUri = new URI(input);
        if (sourceUri.getScheme() == null || sourceUri.getScheme().isEmpty()) {
            sourceUri = Paths.get(input).toUri();
        }
        return sourceUri;
    }
}
