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
import org.springframework.data.mongodb.core.MongoOperations;

import uk.ac.ebi.eva.commons.models.metadata.AnnotationMetadata;
import uk.ac.ebi.eva.pipeline.parameters.AnnotationParameters;
import uk.ac.ebi.eva.pipeline.parameters.DatabaseParameters;

import java.util.List;

/**
 * Tasklet that writes the annotation metadata into mongo. Uses
 * {@link AnnotationMetadata} as the collection schema.
 * <p>
 * Input: VEP version and VEP cache version
 * <p>
 * Output: the collection "annotationMetadata" contains the above parameters.
 */
public class AnnotationMetadataTasklet implements Tasklet {

    @Autowired
    private MongoOperations mongoOperations;

    @Autowired
    private AnnotationParameters annotationParameters;

    @Autowired
    private DatabaseParameters databaseParameters;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        String vepCacheVersion = annotationParameters.getVepCacheVersion();
        String vepVersion = annotationParameters.getVepVersion();
        AnnotationMetadata annotationMetadata = new AnnotationMetadata(vepVersion, vepCacheVersion);
        writeUnlessAlreadyPresent(annotationMetadata);
        return RepeatStatus.FINISHED;
    }

    private void writeUnlessAlreadyPresent(AnnotationMetadata annotationMetadata) {
        String collection = databaseParameters.getCollectionAnnotationMetadataName();
        List<AnnotationMetadata> allMetadata = mongoOperations.findAll(AnnotationMetadata.class);
        long countSameVersion = allMetadata.stream().filter(other -> other.sameVersions(annotationMetadata)).count();
        if (countSameVersion == 0) {
            if (allMetadata.isEmpty()) {
                annotationMetadata.setIsDefault(true);
            }
            mongoOperations.save(annotationMetadata, collection);
        }
    }
}
