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
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;

import uk.ac.ebi.eva.pipeline.parameters.DatabaseParameters;
import uk.ac.ebi.eva.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.pipeline.parameters.MongoConnection;
import uk.ac.ebi.eva.pipeline.parameters.OutputParameters;
import uk.ac.ebi.eva.utils.URLHelper;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Tasklet that loads statistics into mongoDB.
 * <p>
 * Input: file containing statistics (.variants.stats.json.gz)
 * Output: stats loaded into mongodb
 * <p>
 * Example:
 * {
 * "chromosome":"20",
 * "position":67765,
 * "cohortStats":{
 * "ALL":{
 * "refAllele":"C",
 * "altAllele":"T",
 * "variantType":"SNV",
 * "refAlleleCount":4996,
 * "altAlleleCount":12,
 * "genotypesCount":{"0|0":2492,"0|1":12},
 * "missingAlleles":0,
 * "missingGenotypes":0,
 * "refAlleleFreq":0.99760383,
 * "altAlleleFreq":0.002396166,
 * "genotypesFreq":{"0/0":0.0,"0/1":0.0,"1/1":0.0,"0|0":0.99520767,"0|1":0.004792332},
 * "maf":0.002396166,
 * "mgf":0.0,
 * "mafAllele":"T",
 * "mgfGenotype":"0/0",
 * "mendelianErrors":-1,
 * "casesPercentDominant":-1.0,
 * "controlsPercentDominant":-1.0,
 * "casesPercentRecessive":-1.0,
 * "controlsPercentRecessive":-1.0,
 * "quality":100.0,
 * "numSamples":2504
 * }
 * }
 * }
 */
public class LoadStatisticsTasklet implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(LoadStatisticsTasklet.class);

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private OutputParameters outputParameters;

    @Autowired
    private DatabaseParameters dbParameters;

    private JsonFactory jsonFactory;

    private ObjectMapper jsonObjectMapper;

    public LoadStatisticsTasklet() {
        jsonFactory = new JsonFactory();
        jsonObjectMapper = new ObjectMapper(jsonFactory);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        VariantDBAdaptor dbAdaptor = getDbAdaptor();
        URI variantStatsOutputUri = URLHelper.getVariantsStatsUri(
                outputParameters.getOutputDirStatistics(), inputParameters.getStudyId(), inputParameters.getVcfId());
        URI sourceStatsOutputUri = URLHelper.getSourceStatsUri(
                outputParameters.getOutputDirStatistics(), inputParameters.getStudyId(), inputParameters.getVcfId());
        QueryOptions statsOptions = new QueryOptions(getVariantOptions());

        // Load statistics for variants and the file
        loadVariantStats(dbAdaptor, variantStatsOutputUri, statsOptions);
        loadSourceStats(dbAdaptor, sourceStatsOutputUri);

        return RepeatStatus.FINISHED;
    }

    private ObjectMap getVariantOptions() {
        ObjectMap variantOptions = new ObjectMap();
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, getVariantSource());
        variantOptions.put(VariantStorageManager.OVERWRITE_STATS, outputParameters.getStatisticsOverwrite());
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
    private VariantDBAdaptor getDbAdaptor() throws UnknownHostException, IllegalOpenCGACredentialsException {
        MongoCredentials credentials = getMongoCredentials();
        String variantsCollectionName = dbParameters.getCollectionVariantsName();
        String filesCollectionName = dbParameters.getCollectionFilesName();

        logger.debug("Getting DBAdaptor to database '{}'", credentials.getMongoDbName());
        return new VariantMongoDBAdaptor(credentials, variantsCollectionName, filesCollectionName);
    }

    private MongoCredentials getMongoCredentials() throws IllegalOpenCGACredentialsException {
        MongoConnection mongoConnection = dbParameters.getMongoConnection();
        String hosts = mongoConnection.getHosts();
        List<DataStoreServerAddress> dataStoreServerAddresses = MongoCredentials.parseDataStoreServerAddresses(hosts);

        String dbName = dbParameters.getDatabaseName();
        String user = mongoConnection.getUser();
        String pass = mongoConnection.getPassword();

        MongoCredentials mongoCredentials = new MongoCredentials(dataStoreServerAddresses, dbName, user, pass);
        mongoCredentials.setAuthenticationDatabase(mongoConnection.getAuthenticationDatabase());
        return mongoCredentials;
    }

    private void loadVariantStats(VariantDBAdaptor variantDBAdaptor, URI variantsStatsUri, QueryOptions options)
            throws IOException {

        // Open input stream
        InputStream variantInputStream = new GZIPInputStream(new FileInputStream(variantsStatsUri.getPath()));

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
                        statsBatch.get(statsBatch.size() - 1).getChromosome(),
                        statsBatch.get(statsBatch.size() - 1).getPosition());
                statsBatch.clear();
            }
        }

        if (!statsBatch.isEmpty()) {
            QueryResult<?> writeResult = variantDBAdaptor.updateStats(statsBatch, options);
            writes += writeResult.getNumResults();
            logger.info("stats loaded up to position {}:{}",
                    statsBatch.get(statsBatch.size() - 1).getChromosome(),
                    statsBatch.get(statsBatch.size() - 1).getPosition());
            statsBatch.clear();
        }

        if (writes < variantsNumber) {
            logger.warn("provided statistics of {} variants, but only {} were updated", variantsNumber, writes);
            logger.info(
                    "note: maybe those variants didn't had the proper study? maybe the new and the old stats were the same?");
        }
    }

    private void loadSourceStats(VariantDBAdaptor variantDBAdaptor, URI sourceStatsUri) throws IOException {
        // Open input stream
        InputStream sourceInputStream = new GZIPInputStream(new FileInputStream(sourceStatsUri.getPath()));

        // Read from JSON file
        JsonParser sourceParser = jsonFactory.createParser(sourceInputStream);
        VariantSourceStats variantSourceStats = sourceParser.readValueAs(VariantSourceStats.class);

        // Store source statistics in Mongo
        variantDBAdaptor.getVariantSourceDBAdaptor().updateSourceStats(variantSourceStats, null);
    }

}
