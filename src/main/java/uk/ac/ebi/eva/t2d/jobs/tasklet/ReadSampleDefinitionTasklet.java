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
package uk.ac.ebi.eva.t2d.jobs.tasklet;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import uk.ac.ebi.eva.t2d.model.T2DTableStructure;

import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.CONTEXT_TSV_DEFINITION;
import static uk.ac.ebi.eva.utils.T2dTableStructureParser.getTableStructure;

public abstract class ReadSampleDefinitionTasklet implements Tasklet {

    public void storeTableStructureInContext(ChunkContext chunkContext, T2DTableStructure structure) {
        chunkContext.getStepContext().getStepExecution().getExecutionContext()
                .put(CONTEXT_TSV_DEFINITION, structure);
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        T2DTableStructure dataStructure = getTableStructure(getTableName(),
                getTableDefinitionFilePath());
        storeTableStructureInContext(chunkContext, dataStructure);
        return RepeatStatus.FINISHED;
    }

    protected abstract String getTableName();

    protected abstract String getTableDefinitionFilePath();

}
