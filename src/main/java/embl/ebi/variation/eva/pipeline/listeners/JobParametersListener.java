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
package embl.ebi.variation.eva.pipeline.listeners;

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.stereotype.Component;


public class JobParametersListener implements JobExecutionListener {
     
    private static final Logger logger = LoggerFactory.getLogger(JobParametersListener.class);
    
    protected final ObjectMap variantOptions;
    
    public JobParametersListener() {
        variantOptions = new ObjectMap();
    }
    
    @Override
    public void afterJob(JobExecution jobExecution) {
        logger.info("afterJob STATUS + " + jobExecution.getStatus());
        logger.info("afterJob : " + jobExecution);
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        JobParameters parameters = jobExecution.getJobParameters();
        
        logger.info("beforeJob : STARTING");
        logger.info("beforeJob JobParameters : " + parameters);
        
        // TODO validation checks for all the parameters
        Config.setOpenCGAHome(parameters.getString("opencga.app.home"));

        // VariantsLoad configuration
        VariantSource source = new VariantSource(
                parameters.getString("input"), 
                parameters.getString("fileId"),
                parameters.getString("studyId"), 
                parameters.getString("studyName"), 
                VariantStudy.StudyType.valueOf(parameters.getString("studyType")), 
                VariantSource.Aggregation.NONE);
        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);

        // TODO get samples
//                System.out.println(config.samples);
//                System.out.println(config.samples.size());
//                if (config.samples.size() != -3) {
//                    throw new Exception("aborting");
//                }
//                variantOptions.put(VariantStorageManager.SAMPLE_IDS, Arrays.asList(config.samples.split(",")));
        
        variantOptions.put(VariantStorageManager.CALCULATE_STATS, false);   // this is tested by hand
//                variantOptions.put(VariantStorageManager.OVERWRITE_STATS, config.overwriteStats);
        variantOptions.put(VariantStorageManager.INCLUDE_STATS, false);
        
//                variantOptions.put(VariantStorageManager.INCLUDE_GENOTYPES.key(), false);   // TODO rename samples to genotypes
        variantOptions.put(VariantStorageManager.INCLUDE_SAMPLES, true);   // TODO rename samples to genotypes
        variantOptions.put(VariantStorageManager.INCLUDE_SRC, VariantStorageManager.IncludeSrc.parse(parameters.getString("includeSrc")));
        variantOptions.put(VariantStorageManager.COMPRESS_GENOTYPES, Boolean.parseBoolean(parameters.getString("compressGenotypes")));
        
//                variantOptions.put(VariantStorageManager.AGGREGATED_TYPE, VariantSource.Aggregation.NONE);
        variantOptions.put(VariantStorageManager.DB_NAME, parameters.getString("dbName"));
        variantOptions.put(VariantStorageManager.ANNOTATE, false);
//                variantOptions.put(MongoDBVariantStorageManager.LOAD_THREADS, config.loadThreads);
        variantOptions.put("compressExtension", parameters.getString("compressExtension"));

        logger.debug("Using as variantOptions: {}", variantOptions.entrySet().toString());
        logger.debug("Using as input: {}", parameters.getString("input"));
                
//                String storageEngine = parameters.getString("storageEngine");
//                variantStorageManager = StorageManagerFactory.getVariantStorageManager(storageEngine);
//
////                if (config.credentials != null && !config.credentials.isEmpty()) {
////                    variantStorageManager.addConfigUri(new URI(null, config.credentials, null));
////                }
                
//                Path input = Paths.get(nextFileUri.getPath());
//                output = Paths.get(outdirUri.getPath());
//                outputVariantJsonFile = output.resolve(input.getFileName().toString() + ".variants.json" + config.compressExtension);
////                outputFileJsonFile = output.resolve(input.getFileName().toString() + ".file.json" + config.compressExtension);
//                transformedVariantsUri = outdirUri.resolve(outputVariantJsonFile.getFileName().toString());
//
//                // stats config
////                statsOutputUri = outdirUri.resolve(VariantStorageManager.buildFilename(source) + "." + TimeUtils.getTime());  // TODO why was the timestamp required?
//                statsOutputUri = outdirUri.resolve(VariantStorageManager.buildFilename(source));
    }
    
    public ObjectMap getVariantOptions() {
        return variantOptions;
    }
}
