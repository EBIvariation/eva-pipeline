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

package embl.ebi.variation.eva.pipeline.steps;

import embl.ebi.variation.eva.pipeline.ConnectionHelper;
import embl.ebi.variation.eva.pipeline.annotation.GzipLazyResource;
import embl.ebi.variation.eva.pipeline.annotation.load.VariantAnnotationLineMapper;
import embl.ebi.variation.eva.pipeline.annotation.load.VariantAnnotationMongoItemWriter;
import embl.ebi.variation.eva.pipeline.jobs.VariantJobArgsConfig;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.io.IOException;
import java.net.UnknownHostException;

/**
 * @author Diego Poggioli
 *
 * Step class that:
 * - READ: read a list of VEP {@link VariantAnnotation} from flat file
 * - LOAD: write the {@link VariantAnnotation} into Mongo db
 *
 * TODO:
 * - handle the template connection details
 *         https://github.com/opencb/datastore/tree/v0.3.3/datastore-mongodb/src/main/java/org/opencb/datastore/mongodb
 *         or add in the property file: spring.data.mongodb.uri=mongodb://localhost:27017/test
 */

@Configuration
@EnableBatchProcessing
@Import(VariantJobArgsConfig.class)
public class VariantsAnnotLoadBatch {

    public static final String SKIP_ANNOT_LOAD = "skipAnnotLoad";

    @Autowired
    private StepBuilderFactory steps;

    @Autowired
    private ObjectMap pipelineOptions;

    /*
    public static final String jobName = "load";

    @Autowired
    private JobBuilderFactory jobs;

    @Bean
    public Job variantAnnotLoadBatchJob() throws Exception {
        return jobs.get(jobName)
                .start(variantAnnotLoadBatchStep())
                .build();
    }*/

    @Bean
    @Qualifier("variantAnnotLoadBatchStep")
    public Step variantAnnotLoadBatchStep() throws IOException {
        return steps.get("variantAnnotLoadBatchStep").<VariantAnnotation, VariantAnnotation> chunk(10)
                .reader(variantAnnotationReader())
                .writer(variantAnnotationWriter())
                .build();
    }

    @Bean
    public FlatFileItemReader<VariantAnnotation> variantAnnotationReader() throws IOException {
        Resource resource = new GzipLazyResource(pipelineOptions.getString("vepOutput"));
        return initReader(resource);
    }

    public FlatFileItemReader<VariantAnnotation> initReader(Resource resource) {
        FlatFileItemReader<VariantAnnotation> reader = new FlatFileItemReader<>();
        reader.setResource(resource);
        reader.setLineMapper(new VariantAnnotationLineMapper());
        return reader;
    }


    @Bean
    public ItemWriter<VariantAnnotation> variantAnnotationWriter(){
        String dbCollectionVariantsName = pipelineOptions.getString("dbCollectionVariantsName");
        return initWriter(dbCollectionVariantsName, mongoOperations());
    }

    public MongoItemWriter<VariantAnnotation> initWriter(String dbCollectionVariantsName, MongoOperations mongoOperations) {
        MongoItemWriter<VariantAnnotation> writer = new VariantAnnotationMongoItemWriter(mongoOperations);
        writer.setCollection(dbCollectionVariantsName);
        writer.setTemplate(mongoOperations);
        return writer;
    }

    @Bean
    public MongoOperations mongoOperations() {
        MongoTemplate mongoTemplate;
        try {
            mongoTemplate = ConnectionHelper.getMongoTemplate(
                    pipelineOptions.getString("dbHosts"),
                    pipelineOptions.getString("dbAuthenticationDb"),
                    pipelineOptions.getString(VariantStorageManager.DB_NAME),
                    pipelineOptions.getString("dbUser"),
                    pipelineOptions.getString("dbPassword").toCharArray()
            );
        } catch (UnknownHostException e) {
            throw new RuntimeException("Unable to initialize MongoDB", e);
        }
        return mongoTemplate;
    }
}
