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

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoOperations;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.io.readers.UnwindingItemStreamReader;
import uk.ac.ebi.eva.pipeline.io.readers.VcfHeaderReader;
import uk.ac.ebi.eva.pipeline.io.writers.VariantMongoWriter;
import uk.ac.ebi.eva.pipeline.listeners.SkippedItemListener;
import uk.ac.ebi.eva.pipeline.model.converters.data.VariantToMongoDbObjectConverter;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import java.io.File;

/**
 * Step that loads transformed variants into mongoDB
 * <p>
 * Input: transformed variants file (variants.json.gz)
 * Output: variants loaded into mongodb
 */
@Configuration
@EnableBatchProcessing
@Import(JobOptions.class)
public class VariantLoaderStep {
    public static final String LOAD_VARIANTS = "Load variants";

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobOptions jobOptions;

    @Autowired
    VariantSource variantSource;

    @Autowired
    UnwindingItemStreamReader<Variant> unwindingReader;


    @Bean
    @Qualifier("variantsLoadStep")

    public Step variantsLoadStep() throws Exception {
        return stepBuilderFactory.get(LOAD_VARIANTS).<Variant, Variant>chunk(10)
                .reader(unwindingReader)
                .writer(variantMongoWriter())
                .faultTolerant().skipLimit(50).skip(FlatFileParseException.class)
                .listener(new SkippedItemListener())
                .build();
    }

    @Bean
    @StepScope
    public VariantMongoWriter variantMongoWriter() throws Exception {
        MongoOperations mongoOperations = MongoDBHelper
                .getMongoOperations(jobOptions.getDbName(), jobOptions.getMongoConnection());

        return new VariantMongoWriter(jobOptions.getDbCollectionsVariantsName(),
                                      mongoOperations,
                                      variantToMongoDbObjectConverter());
    }

    @Bean
    @StepScope
    public VariantToMongoDbObjectConverter variantToMongoDbObjectConverter() throws Exception {
        return new VariantToMongoDbObjectConverter(
                jobOptions.getVariantOptions().getBoolean(VariantStorageManager.INCLUDE_STATS),
                variantSource.getSamplesPosition(),
                jobOptions.getVariantOptions().getBoolean(VariantStorageManager.CALCULATE_STATS),
                jobOptions.getVariantOptions().getBoolean(VariantStorageManager.INCLUDE_SAMPLES),
                (VariantStorageManager.IncludeSrc) jobOptions.getVariantOptions()
                                                             .get(VariantStorageManager.INCLUDE_SRC));
    }

}
