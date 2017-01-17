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

import com.mongodb.BasicDBObject;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.utils.MongoDBHelper;

/**
 * This step initializes the indexes in the databases.
 * <p>
 * Currently it only has indexes for the features collection.
 */
public class IndexesGeneratorStep implements Tasklet {

    @Autowired
    private JobOptions jobOptions;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        MongoOperations operations = new MongoDBHelper().getMongoOperations(jobOptions.getDbName(),
                                                                            jobOptions.getMongoConnection());
        operations.getCollection(jobOptions.getDbCollectionsFeaturesName())
                .createIndex(new BasicDBObject("name", 1), new BasicDBObject("sparse", true).append("background", true));

        return RepeatStatus.FINISHED;
    }
}
