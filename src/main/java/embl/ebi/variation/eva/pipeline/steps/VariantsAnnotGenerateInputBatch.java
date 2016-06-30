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

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import embl.ebi.variation.eva.pipeline.annotation.generateInput.VariantAnnotationItemProcessor;
import embl.ebi.variation.eva.pipeline.annotation.generateInput.VariantWrapper;
import embl.ebi.variation.eva.pipeline.jobs.VariantJobArgsConfig;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.HashMap;
import java.util.Map;

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
 * - handle the overwrite
 * - handle the template connection details:
 *      https://github.com/opencb/datastore/tree/v0.3.3/datastore-mongodb/src/main/java/org/opencb/datastore/mongodb
 *      or add in the property file: spring.data.mongodb.uri=mongodb://localhost:27017/test
 *
 */

@Configuration
@EnableBatchProcessing
@Import(VariantJobArgsConfig.class)
public class VariantsAnnotGenerateInputBatch {

    @Autowired
    private StepBuilderFactory steps;

    @Autowired
    private ObjectMap pipelineOptions;

/*
    public static final String jobName = "variantsAnnotGenerateInputJob";

    @Bean
    public Step variantsAnnotGenerateInputBatchStep(ItemReader<DBObject> reader,
                      ItemProcessor<DBObject, VepInputLine> processor,
                      ItemWriter<VepInputLine> writer) {
        return steps.get("step1").<DBObject, VepInputLine> chunk(10)
                .reader(reader)
                .processor(processor)
                .writer(writer).allowStartIfComplete(false)
                .build();
    }*/
    @Bean
    @Qualifier("variantsAnnotGenerateInputBatchStep")
    public Step variantsAnnotGenerateInputBatchStep() throws Exception {
        return steps.get("variantsAnnotGenerateInputBatchStep").<DBObject, VariantWrapper> chunk(10)
                .reader(variantReader())
                .processor(vepInputLineProcessor())
                .writer(vepInputWriter()).allowStartIfComplete(false)
                .build();
    }

    @Bean
    public ItemReader<DBObject> variantReader() throws Exception {
        MongoTemplate template =
                new MongoTemplate(new MongoClient(), pipelineOptions.getString(VariantStorageManager.DB_NAME));

        return initReader(pipelineOptions.getString(VariantStorageManager.DB_NAME), template);
    }


    public MongoItemReader<DBObject> initReader(String collection, MongoOperations template){
        MongoItemReader<DBObject> reader = new MongoItemReader<>();
        reader.setCollection(collection);

        reader.setQuery("{ annot : { $exists : false } }");
        reader.setFields("{ chr : 1, start : 1, end : 1, ref : 1, alt : 1, type : 1}");
        reader.setTargetType(DBObject.class);
        reader.setTemplate(template);

        Map<String, Sort.Direction> coordinatesSort = new HashMap<>();
        coordinatesSort.put("chr", Sort.Direction.ASC);
        coordinatesSort.put("start", Sort.Direction.ASC);
        reader.setSort(coordinatesSort);
        return reader;
    }

    @Bean
    public ItemProcessor<DBObject, VariantWrapper> vepInputLineProcessor() {
        return new VariantAnnotationItemProcessor();
    }

    /**
     * @return must return a {@link FlatFileItemWriter} and not a {@link org.springframework.batch.item.ItemWriter}
     * {@see https://jira.spring.io/browse/BATCH-2097
     */
    @Bean
    public FlatFileItemWriter<VariantWrapper> vepInputWriter() throws Exception {
        return initWriter(new FileSystemResource(pipelineOptions.getString("vepInput")));
    }

    public FlatFileItemWriter<VariantWrapper> initWriter(Resource resource){
        BeanWrapperFieldExtractor<VariantWrapper> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] {"chr", "start", "end", "refAlt", "strand"});

        DelimitedLineAggregator<VariantWrapper> delLineAgg = new DelimitedLineAggregator<>();
        delLineAgg.setDelimiter("\t");
        delLineAgg.setFieldExtractor(fieldExtractor);

        FlatFileItemWriter<VariantWrapper> writer = new FlatFileItemWriter<>();

        writer.setResource(resource);
        writer.setAppendAllowed(false);
        writer.setShouldDeleteIfExists(true);
        writer.setLineAggregator(delLineAgg);
        return writer;
    }

}
