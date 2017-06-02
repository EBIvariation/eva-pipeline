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

package uk.ac.ebi.eva.pipeline.configuration.io.writers;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.pipeline.io.writers.VepAnnotationFileWriter;
import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;
import uk.ac.ebi.eva.pipeline.parameters.ChunkSizeParameters;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VEP_ANNOTATION_WRITER;

@Configuration
public class VepAnnotationFileWriterConfiguration {

    @Bean(VEP_ANNOTATION_WRITER)
    @StepScope
    public ItemWriter<EnsemblVariant> vepAnnotationFileWriter(AnnotationParameters annotationParameters,
                                                              ChunkSizeParameters chunkSizeParameters) {
        return new VepAnnotationFileWriter(annotationParameters, chunkSizeParameters.getChunkSize(),
                annotationParameters.getTimeout());
    }

}
