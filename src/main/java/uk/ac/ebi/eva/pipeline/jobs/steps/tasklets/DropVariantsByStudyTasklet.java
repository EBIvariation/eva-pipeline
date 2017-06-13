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
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.FILES_FIELD;

/**
 * Tasklet that removes from mongo all the variants that have only one entry from a given study to delete.
 * If we removed the entry instead of the whole variant, we could end up keeping variants that don't appear in any
 * study (i.e. an empty "files" array in the variant mongo document), which doesn't make sense, so we remove the
 * complete document for those cases first.
 * <p>
 * Input: a studyId
 * <p>
 * Output: those variants are removed
 */
public class DropVariantsByStudyTasklet implements Tasklet {

    private static final Logger logger = LoggerFactory.getLogger(DropVariantsByStudyTasklet.class);

    @Autowired
    private MongoOperations mongoOperations;

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private DatabaseParameters dbParameters;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        String filesStudyIdField = String.format("%s.%s", FILES_FIELD, STUDYID_FIELD);
        Query query = new Query(
                new Criteria(filesStudyIdField).is(inputParameters.getStudyId())
                        .and(FILES_FIELD).size(1));
        logger.info("Deleting variants reported only in study \"{}\"", inputParameters.getStudyId());
        logger.trace("Query used: {}", query);
        WriteResult writeResult = mongoOperations.remove(query, dbParameters.getCollectionVariantsName());
        logger.info("Result: {}", writeResult.toString());

        return RepeatStatus.FINISHED;
    }
}
