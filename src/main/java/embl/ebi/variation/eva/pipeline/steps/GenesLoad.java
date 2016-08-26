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

package embl.ebi.variation.eva.pipeline.steps;

import embl.ebi.variation.eva.pipeline.MongoDBHelper;
import embl.ebi.variation.eva.pipeline.annotation.GzipLazyResource;
import embl.ebi.variation.eva.pipeline.gene.GeneFilterProcessor;
import embl.ebi.variation.eva.pipeline.gene.GeneLineMapper;
import embl.ebi.variation.eva.pipeline.gene.FeatureCoordinates;
import embl.ebi.variation.eva.pipeline.jobs.VariantJobArgsConfig;
import embl.ebi.variation.eva.pipeline.listener.SkipCheckingListener;
import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;
import org.springframework.data.mongodb.core.MongoOperations;

import java.io.IOException;

/**
 * Created by jmmut on 2016-08-16.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 *
 * Step class that:
 * - READ: read a list of {@link FeatureCoordinates} from flat file
 * - LOAD: write the {@link FeatureCoordinates} into Mongo db
 *
 */

@Configuration
@EnableBatchProcessing
@Import(VariantJobArgsConfig.class)
public class GenesLoad {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private ObjectMap pipelineOptions;

    @Bean
    @Qualifier("genesLoadStep")
    public Step genesLoadStep() throws IOException {
        return stepBuilderFactory.get("genesLoadStep").<FeatureCoordinates, FeatureCoordinates>chunk(10)
                .reader(geneReader())
                .processor(geneFilterProcessor())
                .writer(geneWriter())
                .faultTolerant().skipLimit(50).skip(FlatFileParseException.class)
                .listener(skipCheckingListener())
                .build();
    }

    @Bean
    public FlatFileItemReader<FeatureCoordinates> geneReader() throws IOException {
        Resource resource = new GzipLazyResource(pipelineOptions.getString("input.gtf"));
        FlatFileItemReader<FeatureCoordinates> reader = new FlatFileItemReader<>();
        reader.setResource(resource);
        reader.setLineMapper(new GeneLineMapper());
        reader.setComments(new String[] { "#" });   // explicit statement not necessary, it's set up this way by default
        return reader;
    }

    @Bean
    public ItemWriter<FeatureCoordinates> geneWriter(){
        MongoOperations mongoOperations = MongoDBHelper.getMongoOperationsFromPipelineOptions(pipelineOptions);
        MongoItemWriter<FeatureCoordinates> writer = new MongoItemWriter<>();
        writer.setCollection(pipelineOptions.getString("db.collections.features.name"));
        writer.setTemplate(mongoOperations);
        return writer;
    }

    @Bean
    public SkipCheckingListener skipCheckingListener(){
        return new SkipCheckingListener();
    }

    @Bean
    public GeneFilterProcessor geneFilterProcessor(){
        return new GeneFilterProcessor();
    }
}
