/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.configuration;

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Component;
import uk.ac.ebi.eva.pipeline.jobs.flows.AnnotationFlow;
import uk.ac.ebi.eva.pipeline.jobs.flows.PopulationStatisticsFlow;
import uk.ac.ebi.eva.utils.MongoConnection;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Class to extract configuration from properties files and from command line.
 * Default values are in resources/application.properties
 * <p>
 * TODO: 20/05/2016 add type/null/file/dir validators
 * TODO validation checks for all the parameters
 */
@Component
public class JobOptions {
    private static final Logger logger = LoggerFactory.getLogger(JobOptions.class);

    public static final String VEP_INPUT = "vep.input";
    public static final String VEP_OUTPUT = "vep.output";

    // Input
    @Value("${" + JobParametersNames.INPUT_VCF + "}") private String input;
    @Value("${" + JobParametersNames.INPUT_VCF_ID + "}") private String fileId;
    @Value("${" + JobParametersNames.INPUT_VCF_AGGREGATION + "}") private String aggregated;
    @Value("${" + JobParametersNames.INPUT_STUDY_TYPE + "}") private String studyType;
    @Value("${" + JobParametersNames.INPUT_STUDY_NAME + "}") private String studyName;
    @Value("${" + JobParametersNames.INPUT_STUDY_ID + "}") private String studyId;
    @Value("${" + JobParametersNames.INPUT_PEDIGREE + ":}") private String pedigree;
    @Value("${" + JobParametersNames.INPUT_GTF + "}") private String gtf;

    // Output
    @Value("${" + JobParametersNames.OUTPUT_DIR + "}") private String outputDir;
    @Value("${" + JobParametersNames.OUTPUT_DIR_ANNOTATION + "}") private String outputDirAnnotation;
    @Value("${" + JobParametersNames.OUTPUT_DIR_STATISTICS + "}") private String outputDirStatistics;

    @Value("${" + JobParametersNames.STATISTICS_OVERWRITE + ":false}") private boolean overwriteStats;

    @Value("${" + JobParametersNames.APP_OPENCGA_PATH + "}") private String opencgaAppHome;

    //// OpenCGA options with default values (non-customizable)
    private String compressExtension = ".gz";
    private boolean annotate = false;
    private VariantStorageManager.IncludeSrc includeSourceLine = VariantStorageManager.IncludeSrc.FIRST_8_COLUMNS;

    /// DB connection (most parameters read from OpenCGA "conf" folder)
    @Value("${" + JobParametersNames.CONFIG_DB_HOSTS + ":#{null}}") private String dbHosts;
    @Value("${" + JobParametersNames.CONFIG_DB_AUTHENTICATIONDB + ":#{null}}") private String dbAuthenticationDb;
    @Value("${" + JobParametersNames.CONFIG_DB_USER + ":#{null}}") private String dbUser;
    @Value("${" + JobParametersNames.CONFIG_DB_PASSWORD + ":#{null}}") private String dbPassword;
    @Value("${" + JobParametersNames.DB_NAME + ":#{null}}") private String dbName;
    @Value("${" + JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME + ":#{null}}") private String dbCollectionVariantsName;
    @Value("${" + JobParametersNames.DB_COLLECTIONS_FILES_NAME + ":#{null}}") private String dbCollectionFilesName;
    @Value("${" + JobParametersNames.DB_COLLECTIONS_FEATURES_NAME +"}") private String dbCollectionGenesName;
    @Value("${" + JobParametersNames.DB_COLLECTIONS_STATISTICS_NAME + "}") private String dbCollectionStatsName;
    @Value("${" + JobParametersNames.CONFIG_DB_READPREFERENCE + "}") private String readPreference;

    // Skip steps
    @Value("${" + JobParametersNames.ANNOTATION_SKIP + ":false}") private boolean skipAnnot;
    @Value("${" + JobParametersNames.STATISTICS_SKIP + ":false}") private boolean skipStats;

    //VEP
    @Value("${" + JobParametersNames.APP_VEP_PATH +"}") private String vepPath;
    @Value("${" + JobParametersNames.APP_VEP_CACHE_PATH + "}") private String vepCacheDirectory;
    @Value("${" + JobParametersNames.APP_VEP_CACHE_VERSION + "}") private String vepCacheVersion;
    @Value("${" + JobParametersNames.APP_VEP_CACHE_SPECIES + "}") private String vepSpecies;
    @Value("${" + JobParametersNames.INPUT_FASTA + "}") private String vepFasta;
    @Value("${" + JobParametersNames.APP_VEP_NUMFORKS + "}") private String vepNumForks;

    @Value("${" + JobParametersNames.CONFIG_RESTARTABILITY_ALLOW + ":false}") private boolean allowStartIfComplete;

    private ObjectMap variantOptions = new ObjectMap();
    private ObjectMap pipelineOptions = new ObjectMap();
    private File vepInput;
    private File appVepPath;

    @PostConstruct
    public void loadArgs() throws IOException {
        logger.info("Loading job arguments");

        if (opencgaAppHome == null || opencgaAppHome.isEmpty()) {
            opencgaAppHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";
        }
        Config.setOpenCGAHome(opencgaAppHome);

        loadDbConnectionOptions();
        loadOpencgaOptions();
        loadPipelineOptions();
    }

    private void loadDbConnectionOptions() throws IOException {
        URI configUri = URI.create(Config.getOpenCGAHome() + "/").resolve("conf/").resolve("storage-mongodb.properties");
        Properties properties = new Properties();
        properties.load(new InputStreamReader(new FileInputStream(configUri.getPath())));

        if (dbHosts == null) {
            dbHosts = properties.getProperty(JobParametersNames.OPENCGA_DB_HOSTS);
        }
        if (dbAuthenticationDb == null) {
            dbAuthenticationDb = properties.getProperty(JobParametersNames.OPENCGA_DB_AUTHENTICATIONDB, "");
        }
        if (dbUser == null) {
            dbUser = properties.getProperty(JobParametersNames.OPENCGA_DB_USER, "");
        }
        if (dbPassword == null) {
            dbPassword = properties.getProperty(JobParametersNames.OPENCGA_DB_PASSWORD, "");
        }
        if (dbName == null) {
            dbName = properties.getProperty(JobParametersNames.OPENCGA_DB_NAME);
        }
        if (dbCollectionVariantsName == null) {
            dbCollectionVariantsName = properties.getProperty(JobParametersNames.OPENCGA_DB_COLLECTIONS_VARIANTS_NAME, "variants");
        }
        if (dbCollectionFilesName == null) {
            dbCollectionFilesName = properties.getProperty(JobParametersNames.OPENCGA_DB_COLLECTIONS_FILES_NAME, "files");
        }

        if (dbHosts == null || dbHosts.isEmpty()) {
            throw new IllegalArgumentException("Please provide a database hostname");
        }
        if (dbName == null || dbName.isEmpty()) {
            throw new IllegalArgumentException("Please provide a database name");
        }
        if (dbCollectionVariantsName == null || dbCollectionVariantsName.isEmpty()) {
            throw new IllegalArgumentException("Please provide a name for the collection to store the variant information into");
        }
        if (dbCollectionFilesName == null || dbCollectionFilesName.isEmpty()) {
            throw new IllegalArgumentException("Please provide a name for the collection to store the file information into");
        }
    }

    private void loadOpencgaOptions() {
        VariantSource source = new VariantSource(
                Paths.get(input).getFileName().toString(),
                fileId,
                studyId,
                studyName,
                VariantStudy.StudyType.valueOf(studyType),
                VariantSource.Aggregation.valueOf(aggregated));

        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);
        variantOptions.put(VariantStorageManager.OVERWRITE_STATS, overwriteStats);
        variantOptions.put(VariantStorageManager.INCLUDE_SRC, includeSourceLine);
        variantOptions.put("compressExtension", compressExtension);
        variantOptions.put(VariantStorageManager.ANNOTATE, annotate);

        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        variantOptions.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_NAME, dbName);
        variantOptions.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_HOSTS, dbHosts);
        variantOptions.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_AUTHENTICATION_DB, dbAuthenticationDb);
        variantOptions.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_USER, dbUser);
        variantOptions.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_PASS, dbPassword);

        logger.debug("Using as input: {}", input);
        logger.debug("Using as variantOptions: {}", variantOptions.entrySet().toString());
    }

    private void loadPipelineOptions() {
        pipelineOptions.put(JobParametersNames.INPUT_VCF, input);
        pipelineOptions.put("compressExtension", compressExtension);
        pipelineOptions.put(JobParametersNames.OUTPUT_DIR, outputDir);
        pipelineOptions.put(JobParametersNames.OUTPUT_DIR_STATISTICS, outputDirStatistics);
        pipelineOptions.put(JobParametersNames.INPUT_PEDIGREE, pedigree);
        pipelineOptions.put(JobParametersNames.INPUT_GTF, gtf);
        pipelineOptions.put(JobParametersNames.DB_NAME, dbName);
        pipelineOptions.put(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME, dbCollectionVariantsName);
        pipelineOptions.put(JobParametersNames.DB_COLLECTIONS_FILES_NAME, dbCollectionFilesName);
        pipelineOptions.put(JobParametersNames.DB_COLLECTIONS_FEATURES_NAME, dbCollectionGenesName);
        pipelineOptions.put(JobParametersNames.DB_COLLECTIONS_STATISTICS_NAME, dbCollectionStatsName);
        pipelineOptions.put(JobParametersNames.CONFIG_DB_HOSTS, dbHosts);
        pipelineOptions.put(JobParametersNames.CONFIG_DB_AUTHENTICATIONDB, dbAuthenticationDb);
        pipelineOptions.put(JobParametersNames.CONFIG_DB_USER, dbUser);
        pipelineOptions.put(JobParametersNames.CONFIG_DB_PASSWORD, dbPassword);
        pipelineOptions.put(JobParametersNames.CONFIG_DB_READPREFERENCE, readPreference);
        pipelineOptions.put(JobParametersNames.ANNOTATION_SKIP, skipAnnot);
        pipelineOptions.put(JobParametersNames.STATISTICS_SKIP, skipStats);

        String annotationFilesPrefix = studyId + "_" + fileId;
        pipelineOptions.put(VEP_INPUT, URI.create(outputDirAnnotation + "/").resolve(annotationFilesPrefix + "_variants_to_annotate.tsv").toString());
        pipelineOptions.put(VEP_OUTPUT, URI.create(outputDirAnnotation + "/").resolve(annotationFilesPrefix + "_vep_annotation.tsv.gz").toString());

        pipelineOptions.put(JobParametersNames.APP_VEP_PATH, vepPath);
        pipelineOptions.put(JobParametersNames.APP_VEP_CACHE_PATH, vepCacheDirectory);
        pipelineOptions.put(JobParametersNames.APP_VEP_CACHE_VERSION, vepCacheVersion);
        pipelineOptions.put(JobParametersNames.APP_VEP_CACHE_SPECIES, vepSpecies);
        pipelineOptions.put(JobParametersNames.INPUT_FASTA, vepFasta);
        pipelineOptions.put(JobParametersNames.APP_VEP_NUMFORKS, vepNumForks);
        pipelineOptions.put(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW, allowStartIfComplete);

        logger.debug("Using as pipelineOptions: {}", pipelineOptions.entrySet().toString());
    }

    public void configureGenotypesStorage(boolean includeSamples, boolean compressGenotypes) {
        variantOptions.put(VariantStorageManager.INCLUDE_SAMPLES, includeSamples);
        variantOptions.put(VariantStorageManager.COMPRESS_GENOTYPES, compressGenotypes);
    }

    public void configureStatisticsStorage(boolean calculateStats, boolean includeStats) {
        variantOptions.put(VariantStorageManager.CALCULATE_STATS, calculateStats);   // this is tested by hand
        variantOptions.put(VariantStorageManager.INCLUDE_STATS, includeStats);
    }

    public ObjectMap getVariantOptions() {
        return variantOptions;
    }

    public ObjectMap getPipelineOptions() {
        return pipelineOptions;
    }

    public String getDbCollectionsFeaturesName() {
        return getPipelineOptions().getString(JobParametersNames.DB_COLLECTIONS_FEATURES_NAME);
    }

    public String getDbCollectionsVariantsName() {
        return getPipelineOptions().getString(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME);
    }

    public String getDbCollectionsStatsName() {
        return getPipelineOptions().getString(JobParametersNames.DB_COLLECTIONS_STATISTICS_NAME);
    }

    public String getVepInput() {
        return getPipelineOptions().getString(VEP_INPUT);
    }

    public void setVepInputFile(String vepInputFile) {
        getPipelineOptions().put(VEP_INPUT, URI.create(vepInputFile));
    }

    public String getDbName() {
        return getPipelineOptions().getString(JobParametersNames.DB_NAME);
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
        getVariantOptions().put(VariantStorageManager.DB_NAME, dbName);
        getVariantOptions().put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_NAME, dbName);
        getPipelineOptions().put(JobParametersNames.DB_NAME, dbName);
    }

    public String getVepOutput() {
        return getPipelineOptions().getString(VEP_OUTPUT);
    }

    public void setVepOutput(String vepOutput) {
        getPipelineOptions().put(VEP_OUTPUT, URI.create(vepOutput));
    }

    public void setAppVepPath(File appVepPath) {
        getPipelineOptions().put(JobParametersNames.APP_VEP_PATH, appVepPath);
    }

    public String getOutputDir() {
        return getPipelineOptions().getString(JobParametersNames.OUTPUT_DIR);
    }

    public MongoConnection getMongoConnection() {
        MongoConnection connection = new MongoConnection();
        connection.setAuthenticationDatabase(dbAuthenticationDb);
        connection.setHosts(dbHosts);
        connection.setUser(dbUser);
        connection.setPassword(dbPassword);
        connection.setReadPreference(readPreference);
        return connection;
    }

    public MongoOperations getMongoOperations() {
        return MongoDBHelper.getMongoOperationsFromPipelineOptions(getDbName(), getMongoConnection());
    }

}
