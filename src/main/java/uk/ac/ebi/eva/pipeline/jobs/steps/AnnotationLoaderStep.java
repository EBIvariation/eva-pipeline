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

import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.pipeline.configuration.readers.VariantAnnotationReaderConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.writers.VariantAnnotationWriterConfiguration;
import uk.ac.ebi.eva.pipeline.io.readers.AnnotationFlatFileReader;
import uk.ac.ebi.eva.pipeline.io.writers.VepAnnotationMongoWriter;
import uk.ac.ebi.eva.pipeline.listeners.SkippedItemListener;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.LOAD_VEP_ANNOTATION_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_ANNOTATION_READER;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_ANNOTATION_WRITER;

/**
 * This step loads annotations into MongoDB.
 * <p>
 * input: file written by VEP listing annotated variants
 * output: write the annotations into a given variant MongoDB collection.
 * <p>
 * Example file content:
 * 20_60343_G/A	20:60343	A	-	-	-	intergenic_variant	-	-	-	-	-	-
 * 20_60419_A/G	20:60419	G	-	-	-	intergenic_variant	-	-	-	-	-	-
 * 20_60479_C/T	20:60479	T	-	-	-	intergenic_variant	-	-	-	-	-	rs149529999	GMAF=T:0.0018;AFR_MAF=T:0.01;AMR_MAF=T:0.0028
 * <p>
 * each line of the file is loaded with {@link AnnotationFlatFileReader} into a {@link VariantAnnotation} and then sent
 * to mongo with {@link VepAnnotationMongoWriter}.
 */

@Configuration
@EnableBatchProcessing
@Import({VariantAnnotationReaderConfiguration.class, VariantAnnotationWriterConfiguration.class})
public class AnnotationLoaderStep {
    private static final Logger logger = LoggerFactory.getLogger(AnnotationLoaderStep.class);

    @Autowired
    @Qualifier(VARIANT_ANNOTATION_READER)
    private ItemStreamReader<VariantAnnotation> variantAnnotationReader;

    @Autowired
    @Qualifier(VARIANT_ANNOTATION_WRITER)
    private ItemWriter<VariantAnnotation> variantAnnotationItemWriter;

    @Bean(LOAD_VEP_ANNOTATION_STEP)
    public Step loadVepAnnotationStep(StepBuilderFactory stepBuilderFactory, JobOptions jobOptions) {
        logger.debug("Building '" + LOAD_VEP_ANNOTATION_STEP + "'");

        return stepBuilderFactory.get(LOAD_VEP_ANNOTATION_STEP)
                .<VariantAnnotation, VariantAnnotation>chunk(jobOptions.getChunkSize())
                .reader(variantAnnotationReader)
                .writer(variantAnnotationItemWriter)
                .faultTolerant().skipLimit(50).skip(FlatFileParseException.class)
                .allowStartIfComplete(jobOptions.isAllowStartIfComplete())
                .listener(new SkippedItemListener())
                .build();
    }

}
