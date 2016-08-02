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
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Paths;

/**
 *
 * Class to extract configuration from properties files and from command line.
 * Default values are in resources/application.properties
 *
 * @author Diego Poggioli &lt;diego@ebi.ac.uk&gt;
 *
 * TODO: 20/05/2016 add type/null/file/dir validators
 */
@Component
public class VariantJobsArgs {
    private static final Logger logger = LoggerFactory.getLogger(VariantJobsArgs.class);

    ////common
    @Value("${input:}") private String input;
    @Value("${compressExtension}") private String compressExtension;

    ////opencga
    @Value("${fileId}") private String fileId;
    @Value("${studyType}") private String studyType;
    @Value("${studyName}") private String studyName;
    @Value("${studyId}") private String studyId;
    @Value("${compressGenotypes}") private String compressGenotypes;
    @Value("${overwriteStats:false}") private boolean overwriteStats;
    @Value("${calculateStats:false}") private boolean calculateStats;
    @Value("${includeSamples:false}") private String includeSamples;
    @Value("${annotate:false}") private boolean annotate;
    @Value("${includeSrc}") private String includeSrc;
    @Value("${includeStats:false}")private String includeStats;
    @Value("${aggregated}") private String aggregated;

    @Value("${opencga.app.home}") private String opencgaAppHome;

    /// DB connection
    @Value("${dbHosts:}") private String dbHosts;
    @Value("${dbAuthenticationDb:}") private String dbAuthenticationDb;
    @Value("${dbUser:}") private String dbUser;
    @Value("${dbPassword:}") private String dbPassword;
    @Value("${dbName}") private String dbName;
    @Value("${dbCollectionVariantsName}") private String dbCollectionVariantsName;
    @Value("${dbCollectionFilesName}") private String dbCollectionFilesName;
    @Value("${readPreference}") private String readPreference;

    ////pipeline
    @Value("${outputDir}") private String outputDir;
    @Value("${pedigree}") private String pedigree;

    //steps
    @Value("${skipLoad:false}") private boolean skipLoad;
    @Value("${skipStatsCreate:false}") private boolean skipStatsCreate;
    @Value("${skipStatsLoad:false}") private boolean skipStatsLoad;
    @Value("${skipAnnotGenerateInput:false}") private boolean skipAnnotGenerateInput;
    @Value("${skipAnnotCreate:false}") private boolean skipAnnotCreate;
    @Value("${skipAnnotLoad:false}") private boolean skipAnnotLoad;

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

    public void loadArgs() {
        logger.info("Load args");

        // TODO validation checks for all the parameters
        Config.setOpenCGAHome(opencgaAppHome);

        loadVariantOptions();
        loadPipelineOptions();
    }

    private void loadVariantOptions(){
        VariantSource source = new VariantSource(
                Paths.get(input).getFileName().toString(),
                fileId,
                studyId,
                studyName,
                VariantStudy.StudyType.valueOf(studyType),
                VariantSource.Aggregation.valueOf(aggregated));

        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);
        variantOptions.put(VariantStorageManager.OVERWRITE_STATS, overwriteStats);
        variantOptions.put(VariantStorageManager.INCLUDE_SRC, VariantStorageManager.IncludeSrc.parse(includeSrc));
        variantOptions.put("compressExtension", compressExtension);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        variantOptions.put(VariantStorageManager.COMPRESS_GENOTYPES, Boolean.parseBoolean(compressGenotypes));
        variantOptions.put(VariantStorageManager.INCLUDE_STATS, Boolean.parseBoolean(includeStats));

        logger.debug("Using as input: {}", input);

        variantOptions.put(VariantStorageManager.CALCULATE_STATS, false);   // this is tested by hand
        variantOptions.put(VariantStorageManager.INCLUDE_SAMPLES, Boolean.parseBoolean(includeSamples));   // TODO rename samples to genotypes
        variantOptions.put(VariantStorageManager.ANNOTATE, false);

        logger.debug("Using as variantOptions: {}", variantOptions.entrySet().toString());
    }

    private void loadPipelineOptions(){
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
        pipelineOptions.put(VariantsAnnotGenerateInput.SKIP_ANNOT_GENERATE_INPUT, skipAnnotGenerateInput);
        pipelineOptions.put(VariantsAnnotCreate.SKIP_ANNOT_CREATE, skipAnnotCreate);
        pipelineOptions.put(VariantsAnnotLoad.SKIP_ANNOT_LOAD, skipAnnotLoad);
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
