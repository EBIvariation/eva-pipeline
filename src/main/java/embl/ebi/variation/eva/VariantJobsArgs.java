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
package embl.ebi.variation.eva;

import embl.ebi.variation.eva.pipeline.steps.*;
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
import org.springframework.stereotype.Component;

import java.nio.file.Paths;
import java.util.Properties;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageManager;

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

    ////common
    @Value("${input}") private String input;

    // OpenCGA
    @Value("${fileId}") private String fileId;
    @Value("${studyType}") private String studyType;
    @Value("${studyName}") private String studyName;
    @Value("${studyId}") private String studyId;
    @Value("${overwriteStats:false}") private boolean overwriteStats;
    @Value("${aggregated}") private String aggregated;

    @Value("${opencga.app.home}") private String opencgaAppHome;
    
    //// OpenCGA options with default values
    private String compressExtension = ".gz";
    private boolean includeSamples = true;
    private boolean compressGenotypes = true;
    private boolean calculateStats = false;
    private boolean includeStats = false;
    private boolean annotate = false;
    private VariantStorageManager.IncludeSrc includeSourceLine = VariantStorageManager.IncludeSrc.FIRST_8_COLUMNS;

    /// DB connection (most parameters read from OpenCGA "conf" folder)
    private String dbHosts;
    private String dbAuthenticationDb;
    private String dbUser;
    private String dbPassword;
    private String dbName;
    private String dbCollectionVariantsName;
    private String dbCollectionFilesName;
    @Value("${readPreference}") private String readPreference;

    ////pipeline
    @Value("${outputDir}") private String outputDir;
    @Value("${pedigree}") private String pedigree;

    //steps
    @Value("${skipLoad:false}") private boolean skipLoad;
    @Value("${skipStatsCreate:false}") private boolean skipStatsCreate;
    @Value("${skipStatsLoad:false}") private boolean skipStatsLoad;
    @Value("${skipAnnotCreate:false}") private boolean skipAnnotCreate;

    //VEP
    @Value("${vepInput}") private String vepInput;
    @Value("${vepOutput}") private String vepOutput;
    @Value("${vepPath}") private String vepPath;
    @Value("${vepCacheDirectory}") private String vepCacheDirectory;
    @Value("${vepCacheVersion}") private String vepCacheVersion;
    @Value("${vepSpecies}") private String vepSpecies;
    @Value("${vepFasta}") private String vepFasta;
    @Value("${vepNumForks}") private String vepNumForks;

    @Value("${allowStartIfComplete}") private boolean allowStartIfComplete;

    private ObjectMap variantOptions  = new ObjectMap();
    private ObjectMap pipelineOptions  = new ObjectMap();

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
        
        dbHosts = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.HOSTS");
        dbAuthenticationDb = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.AUTHENTICATION.DB", "");
        dbUser = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.USER", "");
        dbPassword = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.PASS", "");
        dbName = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.NAME");
        dbCollectionVariantsName = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.COLLECTION.VARIANTS", "variants");
        dbCollectionFilesName = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.COLLECTION.FILES", "files");
        
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
        pipelineOptions.put("input", input);
        pipelineOptions.put("compressExtension", compressExtension);
        pipelineOptions.put("outputDir", outputDir);
        pipelineOptions.put("pedigree", pedigree);
        pipelineOptions.put("dbHosts", dbHosts);
        pipelineOptions.put("dbAuthenticationDb", dbAuthenticationDb);
        pipelineOptions.put(VariantStorageManager.DB_NAME, dbName);
        pipelineOptions.put("dbCollectionVariantsName", dbCollectionVariantsName);
        pipelineOptions.put("dbCollectionFilesName", dbCollectionFilesName);
        pipelineOptions.put("dbUser", dbUser);
        pipelineOptions.put("dbPassword", dbPassword);
        pipelineOptions.put("readPreference", readPreference);
        pipelineOptions.put(VariantsLoad.SKIP_LOAD, skipLoad);
        pipelineOptions.put(VariantsStatsCreate.SKIP_STATS_CREATE, skipStatsCreate);
        pipelineOptions.put(VariantsStatsLoad.SKIP_STATS_LOAD, skipStatsLoad);
        pipelineOptions.put(VariantsAnnotCreate.SKIP_ANNOT_CREATE, skipAnnotCreate);
        pipelineOptions.put("vepInput", vepInput);
        pipelineOptions.put("vepOutput", vepOutput);
        pipelineOptions.put("vepPath", vepPath);
        pipelineOptions.put("vepCacheDirectory", vepCacheDirectory);
        pipelineOptions.put("vepCacheVersion", vepCacheVersion);
        pipelineOptions.put("vepSpecies", vepSpecies);
        pipelineOptions.put("vepFasta", vepFasta);
        pipelineOptions.put("vepNumForks", vepNumForks);
        pipelineOptions.put("allowStartIfComplete", allowStartIfComplete);

        logger.debug("Using as pipelineOptions: {}", pipelineOptions.entrySet().toString());
    }

    public ObjectMap getVariantOptions() {
        return variantOptions;
    }

    public ObjectMap getPipelineOptions() {
        return pipelineOptions;
    }
}
