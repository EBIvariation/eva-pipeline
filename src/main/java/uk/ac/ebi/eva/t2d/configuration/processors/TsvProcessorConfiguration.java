/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.t2d.configuration.processors;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.t2d.jobs.processors.TsvProcessor;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_TSV_PROCESSOR;

/**
 * Bean definition of the TSV processor. It takes a map of column/values and a mapping file from the job context
 * and generates a list of string values.
 */
@Configuration
public class TsvProcessorConfiguration {

    @Bean(T2D_TSV_PROCESSOR)
    @StepScope
    public TsvProcessor tsvProcessor() {
        return new TsvProcessor();
    }

}
