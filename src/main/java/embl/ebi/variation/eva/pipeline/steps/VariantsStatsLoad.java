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
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

/**
 * Created by jmmut on 2015-11-10.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class VariantsStatsLoad implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(VariantsStatsLoad.class);
    public static final String SKIP_STATS_LOAD = "statistics.load.skip";

    @Autowired
    private ObjectMap variantOptions;

    @Autowired
    private ObjectMap pipelineOptions;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        if (pipelineOptions.getBoolean(SKIP_STATS_LOAD)) {
            logger.info("skipping stats loading");
        } else {
            VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
            QueryOptions statsOptions = new QueryOptions(variantOptions);
            VariantStatisticsManager variantStatisticsManager = new VariantStatisticsManager();
            VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(variantOptions.getString("dbName"), variantOptions);
            URI outdirUri = createUri(pipelineOptions.getString("output.dir"));
            VariantSource variantSource = variantOptions.get(VariantStorageManager.VARIANT_SOURCE, VariantSource.class);
            URI statsOutputUri = outdirUri.resolve(VariantStorageManager.buildFilename(variantSource));

            // actual stats load
            variantStatisticsManager.loadStats(dbAdaptor, statsOutputUri, statsOptions);
        }

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
