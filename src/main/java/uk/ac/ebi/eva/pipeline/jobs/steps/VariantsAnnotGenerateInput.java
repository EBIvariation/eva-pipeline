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
package uk.ac.ebi.eva.pipeline.jobs.steps;

import com.mongodb.DBObject;

import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.context.annotation.Scope;
import uk.ac.ebi.eva.pipeline.configuration.VariantJobsArgs;
import uk.ac.ebi.eva.pipeline.io.readers.VariantReader;
import uk.ac.ebi.eva.pipeline.io.writers.VepInputWriter;
import uk.ac.ebi.eva.pipeline.jobs.steps.processors.VariantAnnotationItemProcessor;
import uk.ac.ebi.eva.pipeline.model.VariantWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * @author Diego Poggioli
 *
 * Dump a list of variants without annotations from mongo
 *
 * Step class that:
 * - READ: read the variants without annotations from mongo
 * - PROCESS convert, filter, validate... the {@link VariantWrapper}
 * - LOAD: write the {@link VariantWrapper} into a flatfile
 *
 * TODO:
 * - Handle the overwrite
 * - The variant list should be compressed. It is not possible to write into a zipped file with FlatFile item writer
 *  see jmmut comment at https://github.com/EBIvariation/eva-v2/pull/22
 *  We can create an extra step to convert the file and remove the nonp-zipped one
 *  https://www.mkyong.com/java/how-to-compress-a-file-in-gzip-format/
 *  https://examples.javacodegeeks.com/core-java/io/fileinputstream/compress-a-file-in-gzip-format-in-java/
 *  http://www.journaldev.com/966/java-gzip-example-compress-and-decompress-file-in-gzip-format-in-java
 */

@Configuration
@EnableBatchProcessing
@Import(VariantJobsArgs.class)
public class VariantsAnnotGenerateInput {

    private static final Logger logger = LoggerFactory.getLogger(VariantsAnnotGenerateInput.class);

    public static final String FIND_VARIANTS_TO_ANNOTATE = "Find variants to annotate";

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private VariantJobsArgs variantJobsArgs;

    @Bean
    @Qualifier("variantsAnnotGenerateInput")
    public Step variantsAnnotGenerateInputBatchStep() throws Exception {
        return stepBuilderFactory.get(FIND_VARIANTS_TO_ANNOTATE).<DBObject, VariantWrapper> chunk(10)
                .reader(new VariantReader(variantJobsArgs.getPipelineOptions()))
                .processor(new VariantAnnotationItemProcessor())
                .writer(new VepInputWriter(variantJobsArgs.getVepInput()))
                .allowStartIfComplete(variantJobsArgs.getPipelineOptions().getBoolean("config.restartability.allow"))
                .build();
    }
}
