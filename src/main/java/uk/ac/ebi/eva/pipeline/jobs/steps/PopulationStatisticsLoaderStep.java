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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.core.config.DataStoreServerAddress;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.json.VariantStatsJsonMixin;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.opencb.opencga.storage.mongodb.variant.VariantMongoDBAdaptor;
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

import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.configuration.JobParametersNames;
import uk.ac.ebi.eva.utils.MongoDBHelper;
import uk.ac.ebi.eva.utils.URLHelper;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Tasklet that loads statistics into mongoDB.
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
@Import({JobOptions.class})
public class PopulationStatisticsLoaderStep implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(PopulationStatisticsLoaderStep.class);

    private static final String VARIANT_STATS_SUFFIX = ".variants.stats.json.gz";

    private static final String SOURCE_STATS_SUFFIX = ".source.stats.json.gz";

    @Autowired
    private JobOptions jobOptions;
    
    private JsonFactory jsonFactory;
    
    private ObjectMapper jsonObjectMapper;

    public PopulationStatisticsLoaderStep() {
        jsonFactory = new JsonFactory();
        jsonObjectMapper = new ObjectMapper(jsonFactory);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
    }
    
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        ObjectMap variantOptions = jobOptions.getVariantOptions();
        ObjectMap pipelineOptions = jobOptions.getPipelineOptions();

        VariantSource variantSource = variantOptions.get(VariantStorageManager.VARIANT_SOURCE, VariantSource.class);
        URI outdirUri = URLHelper.createUri(pipelineOptions.getString(JobParametersNames.OUTPUT_DIR_STATISTICS));
        URI statsOutputUri = outdirUri.resolve(MongoDBHelper.buildStorageFileId(
                variantSource.getStudyId(), variantSource.getFileId()));

        VariantDBAdaptor dbAdaptor = getDbAdaptor(pipelineOptions);
        QueryOptions statsOptions = new QueryOptions(variantOptions);
        
        // Load statistics for variants and the file
        loadVariantStats(dbAdaptor, statsOutputUri, statsOptions);
        loadSourceStats(dbAdaptor, statsOutputUri);

        return RepeatStatus.FINISHED;
    }

    private VariantDBAdaptor getDbAdaptor(ObjectMap properties) throws UnknownHostException, IllegalOpenCGACredentialsException {
        MongoCredentials credentials = getMongoCredentials(properties);
        String variantsCollectionName = properties.getString(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME);
        String filesCollectionName = properties.getString(JobParametersNames.DB_COLLECTIONS_FILES_NAME);

        logger.debug("Getting DBAdaptor to database '{}'", credentials.getMongoDbName());
        return new VariantMongoDBAdaptor(credentials, variantsCollectionName, filesCollectionName);
    }

    private MongoCredentials getMongoCredentials(ObjectMap properties) throws IllegalOpenCGACredentialsException {
        String hosts = properties.getString(JobParametersNames.CONFIG_DB_HOSTS);
        List<DataStoreServerAddress> dataStoreServerAddresses = MongoCredentials.parseDataStoreServerAddresses(hosts);

        String dbName = properties.getString(JobParametersNames.DB_NAME);
        String authenticationDatabase = properties.getString(JobParametersNames.CONFIG_DB_AUTHENTICATIONDB, null);
        String user = properties.getString(JobParametersNames.CONFIG_DB_USER, null);
        String pass = properties.getString(JobParametersNames.CONFIG_DB_PASSWORD, null);

        MongoCredentials mongoCredentials = new MongoCredentials(dataStoreServerAddresses, dbName, user, pass);
        mongoCredentials.setAuthenticationDatabase(authenticationDatabase);
        return mongoCredentials;
    }

    private void loadVariantStats(VariantDBAdaptor variantDBAdaptor, URI uri, QueryOptions options) throws IOException {
        // Open input stream
        Path variantInput = Paths.get(uri.getPath() + VARIANT_STATS_SUFFIX);
        InputStream variantInputStream = new GZIPInputStream(new FileInputStream(variantInput.toFile()));

        // Initialize JSON parser
        JsonParser parser = jsonFactory.createParser(variantInputStream);

        int batchSize = 1000;
        int writes = 0;
        int variantsNumber = 0;
        List<VariantStatsWrapper> statsBatch = new ArrayList<>(batchSize);

        // Store variant statistics in Mongo
        while (parser.nextToken() != null) {
            variantsNumber++;
            statsBatch.add(parser.readValueAs(VariantStatsWrapper.class));

            if (statsBatch.size() == batchSize) {
                QueryResult<?> writeResult = variantDBAdaptor.updateStats(statsBatch, options);
                writes += writeResult.getNumResults();
                logger.info("stats loaded up to position {}:{}", 
                		statsBatch.get(statsBatch.size()-1).getChromosome(), 
                		statsBatch.get(statsBatch.size()-1).getPosition());
                statsBatch.clear();
            }
        }

        if (!statsBatch.isEmpty()) {
            QueryResult<?> writeResult = variantDBAdaptor.updateStats(statsBatch, options);
            writes += writeResult.getNumResults();
            logger.info("stats loaded up to position {}:{}", 
            		statsBatch.get(statsBatch.size()-1).getChromosome(), 
            		statsBatch.get(statsBatch.size()-1).getPosition());
            statsBatch.clear();
        }

        if (writes < variantsNumber) {
            logger.warn("provided statistics of {} variants, but only {} were updated", variantsNumber, writes);
            logger.info("note: maybe those variants didn't had the proper study? maybe the new and the old stats were the same?");
        }
    }

    private void loadSourceStats(VariantDBAdaptor variantDBAdaptor, URI uri) throws IOException {
        // Open input stream
        Path sourceInput = Paths.get(uri.getPath() + SOURCE_STATS_SUFFIX);
        InputStream sourceInputStream = new GZIPInputStream(new FileInputStream(sourceInput.toFile()));

        // Read from JSON file
        JsonParser sourceParser = jsonFactory.createParser(sourceInputStream);
        VariantSourceStats variantSourceStats = sourceParser.readValueAs(VariantSourceStats.class);

        // Store source statistics in Mongo
        variantDBAdaptor.getVariantSourceDBAdaptor().updateSourceStats(variantSourceStats, null);
    }

}
