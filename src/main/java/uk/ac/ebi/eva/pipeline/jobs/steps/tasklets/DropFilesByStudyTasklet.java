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

import com.mongodb.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import uk.ac.ebi.eva.pipeline.parameters.DatabaseParameters;
import uk.ac.ebi.eva.pipeline.parameters.InputParameters;

import static uk.ac.ebi.eva.commons.models.data.VariantSourceEntity.STUDYID_FIELD;

/**
 * Tasklet that removes from mongo all the files that belong to a given study to delete.
 * <p>
 * Input: a studyId
 * <p>
 * Output: all files that are in that study are removed
 */
public class DropFilesByStudyTasklet implements Tasklet {

    private static final Logger logger = LoggerFactory.getLogger(DropFilesByStudyTasklet.class);

    @Autowired
    private MongoOperations mongoOperations;

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private DatabaseParameters dbParameters;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        Query query = new Query(new Criteria(STUDYID_FIELD).is(inputParameters.getStudyId()));

        logger.info("Deleting files from study \"{}\"", inputParameters.getStudyId(),
                dbParameters.getCollectionFilesName());
        logger.trace("Query used: {}", query);
        WriteResult writeResult = mongoOperations.remove(query, dbParameters.getCollectionFilesName());
        logger.info("Result: {}", writeResult.toString());

        return RepeatStatus.FINISHED;
    }
}
