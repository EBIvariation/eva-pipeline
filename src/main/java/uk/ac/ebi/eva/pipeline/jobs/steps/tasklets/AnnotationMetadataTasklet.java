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
package uk.ac.ebi.eva.pipeline.jobs.steps.tasklets;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;

import uk.ac.ebi.eva.commons.models.data.VariantSourceEntity;
import uk.ac.ebi.eva.commons.models.metadata.AnnotationMetadata;
import uk.ac.ebi.eva.pipeline.io.writers.AnnotationMetadataMongoWriter;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;

import java.util.Collections;

/**
 * Tasklet that writes the metadata of a file into mongo. Uses
 * {@link VariantSourceEntity} as the collection schema.
 * <p>
 * Input: VCF file
 * <p>
 * Output: the collection "files" contains the metadata of the VCF.
 */
public class AnnotationMetadataTasklet implements Tasklet {

    @Autowired
    private AnnotationMetadataMongoWriter annotationMetadataMongoWriter;

    @Autowired
    private AnnotationParameters annotationParameters;
    
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        String vepCacheVersion = annotationParameters.getVepCacheVersion();
        String vepVersion = extractVersion(annotationParameters.getVepPath());
        AnnotationMetadata annotationMetadata = new AnnotationMetadata(vepVersion, vepCacheVersion);
        annotationMetadataMongoWriter.write(Collections.singletonList(annotationMetadata));
        return RepeatStatus.FINISHED;
    }

    private String extractVersion(String vepPath) {
        return null;
    }
}
