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
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoOperations;

import uk.ac.ebi.eva.pipeline.configuration.AnnotationLoaderStepConfiguration;
import uk.ac.ebi.eva.pipeline.io.readers.AnnotationFlatFileReader;
import uk.ac.ebi.eva.pipeline.io.writers.VepAnnotationMongoWriter;
import uk.ac.ebi.eva.pipeline.listeners.SkippedItemListener;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import java.io.IOException;

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
@Import({JobOptions.class, AnnotationLoaderStepConfiguration.class})
public class AnnotationLoaderStep {

    public static final String LOAD_VEP_ANNOTATION = "Load VEP annotation";

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobOptions jobOptions;

    @Autowired
    private ItemWriter<VariantAnnotation> variantAnnotationItemWriter;

    @Bean
    @Qualifier("annotationLoad")
    public Step annotationLoadBatchStep() throws IOException {
        MongoOperations mongoOperations = MongoDBHelper.getMongoOperations(
                jobOptions.getDbName(), jobOptions.getMongoConnection());
        String collections = jobOptions.getDbCollectionsVariantsName();
        VepAnnotationMongoWriter writer = new VepAnnotationMongoWriter(mongoOperations, collections);

        return stepBuilderFactory.get(LOAD_VEP_ANNOTATION)
                .<VariantAnnotation, VariantAnnotation>chunk(
                        jobOptions.getPipelineOptions().getInt(JobParametersNames.CONFIG_CHUNK_SIZE))
                .reader(new AnnotationFlatFileReader(jobOptions.getPipelineOptions().getString(JobOptions.VEP_OUTPUT)))
                .writer(variantAnnotationItemWriter)
                .faultTolerant().skipLimit(50).skip(FlatFileParseException.class)
                .listener(new SkippedItemListener())
                .build();
    }

}
