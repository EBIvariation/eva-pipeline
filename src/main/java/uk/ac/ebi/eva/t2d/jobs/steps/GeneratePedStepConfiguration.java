/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.t2d.jobs.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.t2d.configuration.processors.TsvProcessorConfiguration;
import uk.ac.ebi.eva.t2d.configuration.readers.SamplesReaderConfiguration;
import uk.ac.ebi.eva.t2d.configuration.writers.PedWriterConfiguration;
import uk.ac.ebi.eva.t2d.jobs.processors.TsvProcessor;
import uk.ac.ebi.eva.t2d.jobs.readers.TsvReader;
import uk.ac.ebi.eva.t2d.jobs.writers.PedWriter;

import java.util.List;
import java.util.Map;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_GENERATE_PED_STEP;
import static uk.ac.ebi.eva.t2d.BeanNames.T2D_LOAD_SAMPLES_DATA_STEP;
import static uk.ac.ebi.eva.t2d.BeanNames.T2D_PED_WRITER;
import static uk.ac.ebi.eva.t2d.BeanNames.T2D_SAMPLES_READER;
import static uk.ac.ebi.eva.t2d.BeanNames.T2D_TSV_PROCESSOR;

@Configuration
@Profile(Application.T2D_PROFILE)
@EnableBatchProcessing
@Import({SamplesReaderConfiguration.class, TsvProcessorConfiguration.class, PedWriterConfiguration.class})
public class GeneratePedStepConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(GeneratePedStepConfiguration.class);

    @Bean(T2D_GENERATE_PED_STEP)
    public Step prepareDatabaseT2d(StepBuilderFactory stepBuilderFactory, JobOptions jobOptions,
                                   @Qualifier(T2D_SAMPLES_READER) TsvReader loadSamplesFileReader,
                                   @Qualifier(T2D_TSV_PROCESSOR) TsvProcessor tsvProcessor,
                                   @Qualifier(T2D_PED_WRITER) PedWriter pedWriter) {
        logger.debug("Building '" + T2D_GENERATE_PED_STEP + "'");
        return stepBuilderFactory.get(T2D_LOAD_SAMPLES_DATA_STEP)
                .<Map<String, String>, List<String>>chunk(100)
                .reader(loadSamplesFileReader)
                .processor(tsvProcessor)
                .writer(pedWriter)
                .allowStartIfComplete(jobOptions.isAllowStartIfComplete())
                .build();
    }

}
