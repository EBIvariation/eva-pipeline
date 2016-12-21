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

import org.opencb.datastore.core.ObjectMap;
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
import uk.ac.ebi.eva.pipeline.configuration.readers.GeneReaderConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.writers.GeneWriterConfiguration;
import uk.ac.ebi.eva.pipeline.io.mappers.GeneLineMapper;
import uk.ac.ebi.eva.pipeline.io.readers.GeneReader;
import uk.ac.ebi.eva.pipeline.io.writers.GeneWriter;
import uk.ac.ebi.eva.pipeline.jobs.steps.processors.GeneFilterProcessor;
import uk.ac.ebi.eva.pipeline.listeners.SkippedItemListener;
import uk.ac.ebi.eva.pipeline.model.FeatureCoordinates;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.GENES_LOAD_STEP;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.GENE_READER;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.GENE_WRITER;

/**
 * This step loads a list of genomic features from a species into a DB. This DB is intended to be used as a mapping
 * from feature names into feature coordinates (chromosome, start, end).
 * <p>
 * input: GTF file with a list of genomic features.
 * output: writes the features into MongoDB
 * <p>
 * To do so, this step performs the next stages:
 * - reader: To read the file, uses a {@link GeneReader} that fills a {@link FeatureCoordinates} for each line, using a {@link GeneLineMapper}.
 * - processor: Then, filters some, keeping only transcripts and genes.
 * - writer: And later uses a {@link GeneWriter} to load them into mongo.
 */

@Configuration
@EnableBatchProcessing
@Import({GeneReaderConfiguration.class, GeneWriterConfiguration.class})
public class GeneLoaderStep {

    private static final Logger logger = LoggerFactory.getLogger(GeneLoaderStep.class);

    @Autowired
    @Qualifier(GENE_READER)
    private ItemStreamReader<FeatureCoordinates> reader;

    @Autowired
    @Qualifier(GENE_WRITER)
    private ItemWriter<FeatureCoordinates> writer;

    @Bean(GENES_LOAD_STEP)
    public Step genesLoadStep(StepBuilderFactory stepBuilderFactory, JobOptions jobOptions) {
        logger.debug("Building '" + GENES_LOAD_STEP + "'");

        ObjectMap pipelineOptions = jobOptions.getPipelineOptions();
        boolean startIfcomplete = pipelineOptions.getBoolean(JobParametersNames.CONFIG_RESTARTABILITY_ALLOW);

        return stepBuilderFactory.get(GENES_LOAD_STEP)
                .<FeatureCoordinates, FeatureCoordinates>chunk(jobOptions.getPipelineOptions().getInt(JobParametersNames.CONFIG_CHUNK_SIZE))
                .reader(reader)
                .processor(new GeneFilterProcessor())
                .writer(writer)
                .faultTolerant().skipLimit(50).skip(FlatFileParseException.class)
                .allowStartIfComplete(startIfcomplete)
                .listener(new SkippedItemListener())
                .build();
    }

}
