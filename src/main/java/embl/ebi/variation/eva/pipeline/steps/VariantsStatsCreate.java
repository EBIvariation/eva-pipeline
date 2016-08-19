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
public class VariantsStatsCreate implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(VariantsStatsCreate.class);
    public static final String SKIP_STATS_CREATE = "statistics.create.skip";

    @Autowired
    private ObjectMap variantOptions;

    @Autowired
    private ObjectMap pipelineOptions;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
//                HashMap<String, Set<String>> samples = new HashMap<>(); // TODO fill properly. if this is null overwrite will take on
//                samples.put("SOME", new HashSet<>(Arrays.asList("HG00096", "HG00097")));
        //JobParameters parameters = chunkContext.getStepContext().getStepExecution().getJobParameters();

        if (pipelineOptions.getBoolean(SKIP_STATS_CREATE)) {
            logger.info("skipping stats creation step, skipStatsCreate is set to {}" + pipelineOptions.getBoolean(SKIP_STATS_CREATE));
        } else {
            VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
            VariantSource variantSource = variantOptions.get(VariantStorageManager.VARIANT_SOURCE, VariantSource.class);
            VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(variantOptions.getString("dbName"), variantOptions);
            URI outdirUri = createUri(pipelineOptions.getString("output.dir"));
            URI statsOutputUri = outdirUri.resolve(VariantStorageManager.buildFilename(variantSource));

            VariantStatisticsManager variantStatisticsManager = new VariantStatisticsManager();
            QueryOptions statsOptions = new QueryOptions(variantOptions);

            // actual stats creation
            variantStatisticsManager.createStats(dbAdaptor, statsOutputUri, null, statsOptions);    // TODO allow subset of samples
        }

        return RepeatStatus.FINISHED;
    }

    private static URI createUri(String input) throws URISyntaxException {
        URI sourceUri = new URI(input);
        if (sourceUri.getScheme() == null || sourceUri.getScheme().isEmpty()) {
            sourceUri = Paths.get(input).toUri();
        }
        return sourceUri;
    }
}
