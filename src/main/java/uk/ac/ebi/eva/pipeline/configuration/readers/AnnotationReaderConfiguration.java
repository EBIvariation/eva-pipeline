/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.configuration.readers;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.commons.models.mongo.documents.Annotation;
import uk.ac.ebi.eva.pipeline.io.readers.AnnotationFlatFileReader;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_ANNOTATION_READER;

/**
 * Configuration to inject a AnnotationFlatFileReader as a Variant Annotation Reader in the pipeline.
 */
@Configuration
public class AnnotationReaderConfiguration {

    @Bean(VARIANT_ANNOTATION_READER)
    @StepScope
    public ItemStreamReader<Annotation> annotationReader(AnnotationParameters annotationParameters) {
        return new AnnotationFlatFileReader(annotationParameters.getVepOutput(), annotationParameters.getVepVersion(),
                annotationParameters.getVepCacheVersion());
    }

}
