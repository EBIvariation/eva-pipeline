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
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
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

/**
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 *
 * Tasklet that load statistics into mongoDB.
 *
 * Input: file containing statistics (.variants.stats.json.gz)
 * Output: stats loaded into mongodb
 *
 * Example:
 * {
 * "chromosome":"20",
 * "position":67765,
 * "cohortStats":{
 *  "ALL":{
 *      "refAllele":"C",
 *      "altAllele":"T",
 *      "variantType":"SNV",
 *      "refAlleleCount":4996,
 *      "altAlleleCount":12,
 *      "genotypesCount":{"0|0":2492,"0|1":12},
 *      "missingAlleles":0,
 *      "missingGenotypes":0,
 *      "refAlleleFreq":0.99760383,
 *      "altAlleleFreq":0.002396166,
 *      "genotypesFreq":{"0/0":0.0,"0/1":0.0,"1/1":0.0,"0|0":0.99520767,"0|1":0.004792332},
 *      "maf":0.002396166,
 *      "mgf":0.0,
 *      "mafAllele":"T",
 *      "mgfGenotype":"0/0",
 *      "mendelianErrors":-1,
 *      "casesPercentDominant":-1.0,
 *      "controlsPercentDominant":-1.0,
 *      "casesPercentRecessive":-1.0,
 *      "controlsPercentRecessive":-1.0,
 *      "quality":100.0,
 *      "numSamples":2504
 *      }
 *  }
 * }
 */
@Component
@StepScope
@Import({VariantJobsArgs.class})
public class VariantsStatsLoad implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(VariantsStatsLoad.class);

    @Autowired
    private VariantJobsArgs variantJobsArgs;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        ObjectMap variantOptions = variantJobsArgs.getVariantOptions();
        ObjectMap pipelineOptions = variantJobsArgs.getPipelineOptions();

        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        QueryOptions statsOptions = new QueryOptions(variantOptions);
        VariantStatisticsManager variantStatisticsManager = new VariantStatisticsManager();
        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(variantOptions.getString("dbName"), variantOptions);
        URI outdirUri = URLHelper.createUri(pipelineOptions.getString("output.dir"));
        VariantSource variantSource = variantOptions.get(VariantStorageManager.VARIANT_SOURCE, VariantSource.class);
        URI statsOutputUri = outdirUri.resolve(VariantStorageManager.buildFilename(variantSource));

        // actual stats load
        variantStatisticsManager.loadStats(dbAdaptor, statsOutputUri, statsOptions);

        return RepeatStatus.FINISHED;
    }
}
