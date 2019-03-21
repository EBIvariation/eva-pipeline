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
package uk.ac.ebi.eva.pipeline.jobs.steps.tasklets;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;

import uk.ac.ebi.eva.commons.models.data.VariantSourceEntity;
import uk.ac.ebi.eva.pipeline.io.readers.VcfHeaderReader;
import uk.ac.ebi.eva.pipeline.io.writers.VariantSourceEntityMongoWriter;
import uk.ac.ebi.eva.pipeline.parameters.DatabaseParameters;
import uk.ac.ebi.eva.pipeline.parameters.InputParameters;

import java.io.File;
import java.util.Collections;

/**
 * Tasklet that writes the metadata of a file into mongo. Uses
 * {@link uk.ac.ebi.eva.commons.models.data.VariantSourceEntity} as the collection schema.
 * <p>
 * Input: VCF file
 * <p>
 * Output: the collection "files" contains the metadata of the VCF.
 */
public class LoadFileTasklet implements Tasklet {

    @Autowired
    private MongoOperations mongoOperations;

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private DatabaseParameters dbParameters;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        File file = new File(inputParameters.getVcf());

        VcfHeaderReader vcfHeaderReader = new VcfHeaderReader(file,
                inputParameters.getVcfId(),
                inputParameters.getStudyId(),
                inputParameters.getStudyName(),
                inputParameters.getStudyType(),
                inputParameters.getVcfAggregation());
        vcfHeaderReader.open(null);
        VariantSourceEntity variantSourceEntity = vcfHeaderReader.read();

        VariantSourceEntityMongoWriter variantSourceEntityMongoWriter = new VariantSourceEntityMongoWriter(
                mongoOperations, dbParameters.getCollectionFilesName());
        variantSourceEntityMongoWriter.write(Collections.singletonList(variantSourceEntity));

        return RepeatStatus.FINISHED;
    }
}
