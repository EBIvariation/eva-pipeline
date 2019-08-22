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
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

import uk.ac.ebi.eva.commons.mongodb.entities.AnnotationMongo;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;

import java.util.Arrays;
import java.util.List;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ANNOTATION_PARSER_PROCESSOR;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ANNOTATION_COMPOSITE_PROCESSOR;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VEP_ANNOTATION_PROCESSOR;

@Configuration
@Import({VepAnnotationProcessorConfiguration.class, AnnotationParserProcessorConfiguration.class})
public class AnnotationCompositeProcessorConfiguration {

    @Autowired
    @Qualifier(VEP_ANNOTATION_PROCESSOR)
    private ItemProcessor<List<EnsemblVariant>, List<String>> vepAnnotationProcessor;

    @Autowired
    @Qualifier(ANNOTATION_PARSER_PROCESSOR)
    private ItemProcessor<List<String>, List<AnnotationMongo>> annotationParserProcessor;

    @Bean(ANNOTATION_COMPOSITE_PROCESSOR)
    @StepScope
    @Profile(Application.VARIANT_ANNOTATION_MONGO_PROFILE)
    public CompositeItemProcessor<List<EnsemblVariant>, List<AnnotationMongo>> compositeAnnotationItemWriter(){
        CompositeItemProcessor<List<EnsemblVariant>, List<AnnotationMongo>> processor = new CompositeItemProcessor<>();
        processor.setDelegates(Arrays.asList(vepAnnotationProcessor, annotationParserProcessor));
        return processor;
    }
}
