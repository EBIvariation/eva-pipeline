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
package embl.ebi.variation.eva.pipeline.steps.tasklet;

import embl.ebi.variation.eva.VariantJobsArgs;
import embl.ebi.variation.eva.utils.URLHelper;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

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
@StepScope
@Import({VariantJobsArgs.class})
public class VariantsLoad implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(VariantsLoad.class);

    @Autowired
    private VariantJobsArgs variantJobsArgs;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        ObjectMap variantOptions = variantJobsArgs.getVariantOptions();
        ObjectMap pipelineOptions = variantJobsArgs.getPipelineOptions();

        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();// TODO add mongo
        URI outdirUri = URLHelper.createUri(pipelineOptions.getString("output.dir"));
        URI nextFileUri = URLHelper.createUri(pipelineOptions.getString("input.vcf"));

//          URI pedigreeUri = pipelineOptions.getString("input.pedigree") != null ? createUri(pipelineOptions.getString("input.pedigree")) : null;
        Path output = Paths.get(outdirUri.getPath());
        Path input = Paths.get(nextFileUri.getPath());
        Path outputVariantJsonFile = output.resolve(input.getFileName().toString() + ".variants.json" + pipelineOptions.getString("compressExtension"));
//          outputFileJsonFile = output.resolve(input.getFileName().toString() + ".file.json" + config.compressExtension);
        URI transformedVariantsUri = outdirUri.resolve(outputVariantJsonFile.getFileName().toString());

        logger.info("-- PreLoad variants -- {}", nextFileUri);
        variantStorageManager.preLoad(transformedVariantsUri, outdirUri, variantOptions);
        logger.info("-- Load variants -- {}", nextFileUri);
        variantStorageManager.load(transformedVariantsUri, variantOptions);
//          logger.info("-- PostLoad variants -- {}", nextFileUri);
//          variantStorageManager.postLoad(transformedVariantsUri, outdirUri, variantOptions);

        return RepeatStatus.FINISHED;
    }

}
