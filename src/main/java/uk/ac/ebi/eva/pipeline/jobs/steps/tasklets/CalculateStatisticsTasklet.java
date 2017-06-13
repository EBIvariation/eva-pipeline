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
package uk.ac.ebi.eva.pipeline.jobs.steps.tasklets;

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;

import uk.ac.ebi.eva.pipeline.parameters.ChunkSizeParameters;
import uk.ac.ebi.eva.pipeline.parameters.DatabaseParameters;
import uk.ac.ebi.eva.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.pipeline.parameters.MongoConnection;
import uk.ac.ebi.eva.pipeline.parameters.OutputParameters;
import uk.ac.ebi.eva.utils.URLHelper;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

/**
 * Tasklet that calculates statistics. See {@link org.opencb.biodata.models.variant.stats.VariantStats} for a list of
 * fields that are calculated for each subset of samples.
 * <p>
 * Input: variants loaded into mongodb
 * Output: file containing statistics (.variants.stats.json.gz)
 */
public class CalculateStatisticsTasklet implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(CalculateStatisticsTasklet.class);

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private ChunkSizeParameters chunkSizeParameters;

    @Autowired
    private OutputParameters outputParameters;

    @Autowired
    private DatabaseParameters dbParameters;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
//                HashMap<String, Set<String>> samples = new HashMap<>(); // TODO fill properly. if this is null overwrite will take on
//                samples.put("SOME", new HashSet<>(Arrays.asList("HG00096", "HG00097")));

        ObjectMap variantOptions = getVariantOptions();
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(dbParameters.getDatabaseName(), variantOptions);
        URI statsOutputUri = getStatsBaseUri();

        VariantStatisticsManager variantStatisticsManager = new VariantStatisticsManager();
        QueryOptions statsOptions = new QueryOptions(variantOptions);

        // actual stats creation
        variantStatisticsManager.createStats(dbAdaptor, statsOutputUri, null, statsOptions);    // TODO allow subset of samples

        return RepeatStatus.FINISHED;
    }

    private URI getStatsBaseUri() throws URISyntaxException {
        return URLHelper.getStatsBaseUri(
                outputParameters.getOutputDirStatistics(), inputParameters.getStudyId(), inputParameters.getVcfId());
    }

    private ObjectMap getVariantOptions() {

        VariantSource source = getVariantSource();

        // OpenCGA options with default values (non-customizable)
        String compressExtension = ".gz";
        boolean annotate = false;
        VariantStorageManager.IncludeSrc includeSourceLine = VariantStorageManager.IncludeSrc.FIRST_8_COLUMNS;

        ObjectMap variantOptions = new ObjectMap();
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);
        variantOptions.put(VariantStorageManager.OVERWRITE_STATS, outputParameters.getStatisticsOverwrite());
        variantOptions.put(VariantStorageManager.INCLUDE_SRC, includeSourceLine);
        variantOptions.put("compressExtension", compressExtension);
        variantOptions.put(VariantStorageManager.ANNOTATE, annotate);
        variantOptions.put(VariantStatisticsManager.BATCH_SIZE, chunkSizeParameters.getChunkSize());

        variantOptions.put(VariantStorageManager.DB_NAME, dbParameters.getDatabaseName());
        MongoConnection mongoConnection = dbParameters.getMongoConnection();
        variantOptions.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_NAME,
                dbParameters.getDatabaseName());
        variantOptions.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_HOSTS,
                mongoConnection.getHosts());
        variantOptions.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_AUTHENTICATION_DB,
                mongoConnection.getAuthenticationDatabase());
        variantOptions.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_USER,
                mongoConnection.getUser());
        variantOptions.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_PASS,
                mongoConnection.getPassword());

        return variantOptions;
    }

    private VariantSource getVariantSource() {
        return new VariantSource(
                    Paths.get(inputParameters.getVcf()).getFileName().toString(),
                    inputParameters.getVcfId(),
                    inputParameters.getStudyId(),
                    inputParameters.getStudyName(),
                    inputParameters.getStudyType(),
                    inputParameters.getVcfAggregation());
    }
}
