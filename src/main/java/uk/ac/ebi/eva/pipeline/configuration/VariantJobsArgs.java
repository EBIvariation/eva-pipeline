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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Component;

import uk.ac.ebi.eva.pipeline.jobs.VariantAnnotConfiguration;
import uk.ac.ebi.eva.pipeline.jobs.VariantStatsConfiguration;
import uk.ac.ebi.eva.pipeline.jobs.steps.VariantsAnnotCreate;

import java.nio.file.Paths;
import java.util.Properties;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageManager;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import javax.annotation.PostConstruct;

/**
 *
 * Class to extract configuration from properties files and from command line.
 * Default values are in resources/application.properties
 *
 * @author Diego Poggioli &lt;diego@ebi.ac.uk&gt;
 *
 * TODO: 20/05/2016 add type/null/file/dir validators
 * TODO validation checks for all the parameters
 */
@Component
public class VariantJobsArgs {
    private static final Logger logger = LoggerFactory.getLogger(VariantJobsArgs.class);
    private static final String DB_COLLECTIONS_FEATURES_NAME = "db.collections.features.name";
    private static final String DB_COLLECTIONS_VARIANTS_NAME = "db.collections.variants.name";
    private static final String VEP_INPUT = "vep.input";
    private static final String DB_NAME = "db.name";
    private static final String VEP_OUTPUT = "vep.output";
    private static final String APP_VEP_PATH = "app.vep.path";

    // Input
    @Value("${input.vcf}") private String input;
    @Value("${input.vcf.id}") private String fileId;
    @Value("${input.vcf.aggregation}") private String aggregated;
    @Value("${input.study.type}") private String studyType;
    @Value("${input.study.name}") private String studyName;
    @Value("${input.study.id}") private String studyId;
    @Value("${input.pedigree:}") private String pedigree;
    @Value("${input.gtf}") private String gtf;
    
    // Output
    @Value("${output.dir}") private String outputDir;
    @Value("${output.dir.annotation}") private String outputDirAnnotation;
    @Value("${output.dir.statistics}") private String outputDirStatistics;

    @Value("${statistics.overwrite:false}") private boolean overwriteStats;

    @Value("${app.opencga.path}") private String opencgaAppHome;
    
    //// OpenCGA options with default values (non-customizable)
    private String compressExtension = ".gz";
    private boolean includeSamples = true;
    private boolean compressGenotypes = true;
    private boolean calculateStats = false;
    private boolean includeStats = false;
    private boolean annotate = false;
    private VariantStorageManager.IncludeSrc includeSourceLine = VariantStorageManager.IncludeSrc.FIRST_8_COLUMNS;

    /// DB connection (most parameters read from OpenCGA "conf" folder)
    @Value("${config.db.hosts:#{null}}") private String dbHosts;
    @Value("${config.db.authentication-db:#{null}}") private String dbAuthenticationDb;
    @Value("${config.db.user:#{null}}") private String dbUser;
    @Value("${config.db.password:#{null}}") private String dbPassword;
    @Value("${"+DB_NAME+":#{null}}") private String dbName;
    @Value("${"+DB_COLLECTIONS_VARIANTS_NAME+":#{null}}") private String dbCollectionVariantsName;
    @Value("${db.collections.files.name:#{null}}") private String dbCollectionFilesName;
    @Value("${"+DB_COLLECTIONS_FEATURES_NAME+"}") private String dbCollectionGenesName;
    @Value("${config.db.read-preference}") private String readPreference;

    // Skip steps
    @Value("${annotation.skip:false}") private boolean skipAnnot;
    @Value("${statistics.skip:false}") private boolean skipStats;

    //VEP
    @Value("${"+APP_VEP_PATH+"}") private String vepPath;
    @Value("${app.vep.cache.path}") private String vepCacheDirectory;
    @Value("${app.vep.cache.version}") private String vepCacheVersion;
    @Value("${app.vep.cache.species}") private String vepSpecies;
    @Value("${input.fasta}") private String vepFasta;
    @Value("${app.vep.num-forks}") private String vepNumForks;

    @Value("${config.restartability.allow:false}") private boolean allowStartIfComplete;

    private ObjectMap variantOptions  = new ObjectMap();
    private ObjectMap pipelineOptions  = new ObjectMap();
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
            dbHosts = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.HOSTS");
        }
        if (dbAuthenticationDb == null) {
            dbAuthenticationDb = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.AUTHENTICATION.DB", "");
        }
        if (dbUser == null) {
            dbUser = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.USER", "");
        }
        if (dbPassword == null) {
            dbPassword = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.PASS", "");
        }
        if (dbName == null) {
            dbName = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.NAME");
        }
        if (dbCollectionVariantsName == null) {
            dbCollectionVariantsName = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.COLLECTION.VARIANTS", "variants");
        }
        if (dbCollectionFilesName == null) {
            dbCollectionFilesName = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.COLLECTION.FILES", "files");
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
        variantOptions.put(VariantStorageManager.INCLUDE_SAMPLES, includeSamples);   // TODO rename samples to genotypes
        variantOptions.put(VariantStorageManager.COMPRESS_GENOTYPES, compressGenotypes);
        variantOptions.put(VariantStorageManager.CALCULATE_STATS, calculateStats);   // this is tested by hand
        variantOptions.put(VariantStorageManager.INCLUDE_STATS, includeStats);
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
        pipelineOptions.put("input.vcf", input);
        pipelineOptions.put("compressExtension", compressExtension);
        pipelineOptions.put("output.dir", outputDir);
        pipelineOptions.put("output.dir.statistics", outputDirStatistics);
        pipelineOptions.put("input.pedigree", pedigree);
        pipelineOptions.put("input.gtf", gtf);
        pipelineOptions.put(DB_NAME, dbName);
        pipelineOptions.put(DB_COLLECTIONS_VARIANTS_NAME, dbCollectionVariantsName);
        pipelineOptions.put("db.collections.files.name", dbCollectionFilesName);
        pipelineOptions.put(DB_COLLECTIONS_FEATURES_NAME, dbCollectionGenesName);
        pipelineOptions.put("config.db.hosts", dbHosts);
        pipelineOptions.put("config.db.authentication-db", dbAuthenticationDb);
        pipelineOptions.put("config.db.user", dbUser);
        pipelineOptions.put("config.db.password", dbPassword);
        pipelineOptions.put("config.db.read-preference", readPreference);
        pipelineOptions.put(VariantAnnotConfiguration.SKIP_ANNOT, skipAnnot);
        pipelineOptions.put(VariantStatsConfiguration.SKIP_STATS, skipStats);

        String annotationFilesPrefix = studyId + "_" + fileId;
        pipelineOptions.put(VEP_INPUT, URI.create(outputDirAnnotation + "/").resolve(annotationFilesPrefix + "_variants_to_annotate.tsv").toString());
        pipelineOptions.put(VEP_OUTPUT, URI.create(outputDirAnnotation + "/").resolve(annotationFilesPrefix + "_vep_annotation.tsv.gz").toString());
        
        pipelineOptions.put(APP_VEP_PATH, vepPath);
        pipelineOptions.put("app.vep.cache.path", vepCacheDirectory);
        pipelineOptions.put("app.vep.cache.version", vepCacheVersion);
        pipelineOptions.put("app.vep.cache.species", vepSpecies);
        pipelineOptions.put("input.fasta", vepFasta);
        pipelineOptions.put("app.vep.num-forks", vepNumForks);
        pipelineOptions.put("config.restartability.allow", allowStartIfComplete);

        logger.debug("Using as pipelineOptions: {}", pipelineOptions.entrySet().toString());
    }

    public ObjectMap getVariantOptions() {
        return variantOptions;
    }

    public ObjectMap getPipelineOptions() {
        return pipelineOptions;
    }

    public MongoOperations getMongoOperations() {
        return MongoDBHelper.getMongoOperationsFromPipelineOptions(getPipelineOptions());
    }

    public String getDbCollectionsFeaturesName() {
        return getPipelineOptions().getString(DB_COLLECTIONS_FEATURES_NAME);
    }

    public String getDbCollectionsVariantsName() {
        return getPipelineOptions().getString(DB_COLLECTIONS_VARIANTS_NAME);
    }

    public String getVepInput() {
        return getPipelineOptions().getString(VEP_INPUT);
    }

    public void setVepInputFile(String vepInputFile) {
        getPipelineOptions().put(VEP_INPUT, URI.create(vepInputFile));
    }

    public String getDbName() {
        return getPipelineOptions().getString(DB_NAME);
    }

    public String getVepOutput() {
        return getPipelineOptions().getString(VEP_OUTPUT);
    }

    public void setVepOutput(String vepOutput){
        getPipelineOptions().put(VEP_OUTPUT, URI.create(vepOutput));
    }

    public void setAppVepPath(File appVepPath) {
        getPipelineOptions().put(APP_VEP_PATH, appVepPath);
    }

}
