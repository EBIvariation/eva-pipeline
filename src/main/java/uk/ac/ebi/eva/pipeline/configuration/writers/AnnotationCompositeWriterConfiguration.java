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
package uk.ac.ebi.eva.pipeline.configuration.writers;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

import uk.ac.ebi.eva.commons.models.data.Annotation;
import uk.ac.ebi.eva.pipeline.Application;

import java.util.Arrays;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.COMPOSITE_ANNOTATION_VARIANT_WRITER;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ANNOTATION_IN_VARIANT_WRITER;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.ANNOTATION_WRITER;

@Configuration
@Import({AnnotationWriterConfiguration.class, AnnotationInVariantWriterConfiguration.class})
public class AnnotationCompositeWriterConfiguration {

    @Autowired
    @Qualifier(ANNOTATION_WRITER)
    private ItemWriter<Annotation> annotationItemWriter;

    @Autowired
    @Qualifier(ANNOTATION_IN_VARIANT_WRITER)
    private ItemWriter<Annotation> variantAnnotationItemWriter;

    @Bean(COMPOSITE_ANNOTATION_VARIANT_WRITER)
    @StepScope
    @Profile(Application.VARIANT_ANNOTATION_MONGO_PROFILE)
    public CompositeItemWriter<Annotation> compositeAnnotationItemWriter(){
        CompositeItemWriter writer = new CompositeItemWriter();
        writer.setDelegates(Arrays.asList(annotationItemWriter, variantAnnotationItemWriter));
        return writer;
    }
}
