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
package uk.ac.ebi.eva.t2d.configuration.writers;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.t2d.jobs.writers.T2dVepAnnotationFileWriter;
import uk.ac.ebi.eva.t2d.model.T2dAnnotation;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_ANNOTATION_LOAD_WRITER;

/**
 * Bean definition for the Annotation load process
 */
@Configuration
public class T2dAnnotationLoadWriterConfiguration {

    @Bean(T2D_ANNOTATION_LOAD_WRITER)
    @StepScope
    public ItemWriter<T2dAnnotation> vepAnnotationFileWriter() {
        return new T2dVepAnnotationFileWriter();
    }

}
