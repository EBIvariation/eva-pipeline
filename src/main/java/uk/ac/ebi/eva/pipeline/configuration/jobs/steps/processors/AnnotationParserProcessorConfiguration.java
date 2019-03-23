/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.configuration.jobs.steps.processors;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.commons.mongodb.entities.AnnotationMongo;
import uk.ac.ebi.eva.pipeline.jobs.steps.processors.AnnotationParserProcessor;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;

import java.util.List;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ANNOTATION_PARSER_PROCESSOR;

/**
 * Step that annotates a list of variant coordinates (EnsemblVariant)
 * <p>
 * Input: List of EnsemblVariant
 * Output: List of Strings, each string is an output line from VEP
 */
@Configuration
public class AnnotationParserProcessorConfiguration {

    @Bean(ANNOTATION_PARSER_PROCESSOR)
    @StepScope
    public ItemProcessor<List<String>, List<AnnotationMongo>> annotationParserProcessor(
            AnnotationParameters annotationParameters) {
        return new AnnotationParserProcessor(annotationParameters);
    }

}
