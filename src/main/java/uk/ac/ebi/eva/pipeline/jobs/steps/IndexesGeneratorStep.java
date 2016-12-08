/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.jobs.steps;

import com.mongodb.BasicDBObject;
import org.opencb.datastore.core.ObjectMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Component;

import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.utils.MongoDBHelper;

/**
 * This step initializes the indexes in the databases.
 * <p>
 * Currently it only has indexes for the features collection.
 */
@Component
@StepScope
@Import({JobOptions.class})
public class IndexesGeneratorStep implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(IndexesGeneratorStep.class);

    @Autowired
    private JobOptions jobOptions;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        ObjectMap pipelineOptions = jobOptions.getPipelineOptions();
        MongoOperations operations = MongoDBHelper.getMongoOperations(jobOptions.getDbName(),
                jobOptions.getMongoConnection());
        operations.getCollection(jobOptions.getDbCollectionsFeaturesName())
                .createIndex(new BasicDBObject("name", 1), new BasicDBObject("sparse", true).append("background", true));

        return RepeatStatus.FINISHED;
    }
}
