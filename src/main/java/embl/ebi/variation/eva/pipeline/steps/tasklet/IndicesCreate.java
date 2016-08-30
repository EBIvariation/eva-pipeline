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
package embl.ebi.variation.eva.pipeline.steps.tasklet;

import com.mongodb.BasicDBObject;
import embl.ebi.variation.eva.VariantJobsArgs;
import embl.ebi.variation.eva.pipeline.MongoDBHelper;
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

/**
 * Created by jmmut on 2016-08-26.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
@Component
@StepScope
@Import({VariantJobsArgs.class})
public class IndicesCreate implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(IndicesCreate.class);
    public static final String SKIP_INITIALIZE_INDICES = "initialize.indices.skip";

    @Autowired
    private VariantJobsArgs variantJobsArgs;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        ObjectMap pipelineOptions = variantJobsArgs.getPipelineOptions();
        if (pipelineOptions.getBoolean(SKIP_INITIALIZE_INDICES)) {
            logger.info("skipping indices creation step, initialize.indices.skip is set to {} ",
                    pipelineOptions.getBoolean(SKIP_INITIALIZE_INDICES));
        } else {
            MongoOperations operations = MongoDBHelper.getMongoOperationsFromPipelineOptions(pipelineOptions);
            operations.getCollection(pipelineOptions.getString("db.collections.features.name"))
                    .createIndex(new BasicDBObject("name", 1), new BasicDBObject("sparse", true));
        }

        return RepeatStatus.FINISHED;
    }
}
