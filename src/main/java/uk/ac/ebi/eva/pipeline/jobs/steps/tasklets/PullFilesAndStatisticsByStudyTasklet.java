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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
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
import org.springframework.data.mongodb.core.query.Update;

import uk.ac.ebi.eva.pipeline.parameters.DatabaseParameters;
import uk.ac.ebi.eva.pipeline.parameters.InputParameters;

import static uk.ac.ebi.eva.commons.models.data.VariantSourceEntity.STUDYID_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.FILES_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.STATS_FIELD;

/**
 * Tasklet that removes the files and statistics in a variant given a studyId. The id is readed from the jobParameter
 * studyId.
 */
public class PullFilesAndStatisticsByStudyTasklet implements Tasklet {

    private static final Logger logger = LoggerFactory.getLogger(
            PullFilesAndStatisticsByStudyTasklet.class);

    @Autowired
    private MongoOperations mongoOperations;

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private DatabaseParameters dbParameters;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        logger.info("Pulling files and statistics from variants in study \"{}\"",
                inputParameters.getStudyId());
        dropVariantsAndStatisticsByStudy(inputParameters.getStudyId());
        return RepeatStatus.FINISHED;
    }

    private void dropVariantsAndStatisticsByStudy(String studyId) {
        String filesStudyIdField = String.format("%s.%s", FILES_FIELD, STUDYID_FIELD);
        Query query = Query.query(Criteria.where(filesStudyIdField).is(studyId));

        DBObject containsStudyId = new BasicDBObject(STUDYID_FIELD, studyId);
        Update update = new Update().pull(FILES_FIELD, containsStudyId).pull(STATS_FIELD, containsStudyId);

        logger.trace("Update operation with Query : {} and Update: {}", query, update);
        WriteResult writeResult = mongoOperations.updateMulti(query, update, dbParameters.getCollectionVariantsName());
        logger.info("Result: {}", writeResult.toString());

    }
}
