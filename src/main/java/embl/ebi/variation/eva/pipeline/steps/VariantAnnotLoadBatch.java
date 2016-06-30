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

import com.mongodb.MongoClient;
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
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;

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
public class VariantAnnotLoadBatch {

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
    public Step variantAnnotLoadBatchStep(){
        return steps.get("variantAnnotLoadBatchStep").<VariantAnnotation, VariantAnnotation> chunk(10)
                .reader(variantAnnotationReader())
                .writer(variantAnnotationWriter())
                .build();
    }

	@Bean
	public FlatFileItemReader<VariantAnnotation> variantAnnotationReader() {
		FlatFileItemReader<VariantAnnotation> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource(pipelineOptions.getString("vepOutput")));
        reader.setLineMapper(new VariantAnnotationLineMapper());
		return reader;
	}

	@Bean
	public ItemWriter<VariantAnnotation> variantAnnotationWriter(){
		MongoItemWriter<VariantAnnotation> writer = new VariantAnnotationMongoItemWriter(mongoOperations());
		writer.setCollection(pipelineOptions.getString(VariantStorageManager.DB_NAME));
		writer.setTemplate(mongoOperations());
		return writer;
	}

    @Bean
    public MongoOperations mongoOperations() {
        MongoTemplate mongoTemplate;
        try {
            mongoTemplate = new MongoTemplate(new MongoClient(), pipelineOptions.getString(VariantStorageManager.DB_NAME));
        } catch (UnknownHostException e) {
            throw new RuntimeException("Unable to initialize MongoDB", e);
        }
        return mongoTemplate;
    }
	
}
