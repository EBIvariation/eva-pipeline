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
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoOperations;

import uk.ac.ebi.eva.pipeline.MongoDBHelper;
import uk.ac.ebi.eva.pipeline.configuration.VariantJobsArgs;
import uk.ac.ebi.eva.pipeline.io.readers.VariantAnnotationReader;
import uk.ac.ebi.eva.pipeline.io.writers.VariantAnnotationMongoItemWriter;
import uk.ac.ebi.eva.pipeline.listener.SkipCheckingListener;

import java.io.IOException;

/**
 * @author Diego Poggioli
 *
 * Step class that:
 * - READ: read a list of VEP {@link VariantAnnotation} from flat file
 * - LOAD: write the {@link VariantAnnotation} into Mongo db
 *
 */

@Configuration
@EnableBatchProcessing
@Import({VariantJobsArgs.class})
public class VariantsAnnotLoad {

    public static final String LOAD_VEP_ANNOTATION = "Load VEP annotation";

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private VariantJobsArgs variantJobsArgs;

    @Bean
    @Qualifier("variantAnnotLoad")
    public Step variantAnnotLoadBatchStep() throws IOException {
        MongoOperations mongoOperations = MongoDBHelper.getMongoOperationsFromPipelineOptions(variantJobsArgs.getPipelineOptions());
        String collections = variantJobsArgs.getPipelineOptions().getString("db.collections.variants.name");
        VariantAnnotationMongoItemWriter writer = new VariantAnnotationMongoItemWriter(mongoOperations, collections);

        return stepBuilderFactory.get(LOAD_VEP_ANNOTATION).<VariantAnnotation, VariantAnnotation> chunk(10)
                .reader(new VariantAnnotationReader(variantJobsArgs.getPipelineOptions()))
                .writer(writer)
                .faultTolerant().skipLimit(50).skip(FlatFileParseException.class)
                .listener(new SkipCheckingListener())
                .build();
    }

}
