/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
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
import uk.ac.ebi.eva.t2d.parameters.T2dMetadataParameters;
import uk.ac.ebi.eva.t2d.parameters.T2dTsvParameters;
import uk.ac.ebi.eva.t2d.services.T2dService;
import uk.ac.ebi.eva.utils.T2dTableStructureParser;

import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.CONTEXT_TSV_DEFINITION;

/**
 * Tasklet to handle metadata insertion and table creation
 */
public abstract class PrepareDatabaseTasklet implements Tasklet {

    private final T2dService service;

    private final T2dMetadataParameters metadataParameters;

    private final T2dTsvParameters tsvParameters;

    public PrepareDatabaseTasklet(T2dService service,
                                  T2dMetadataParameters metadataParameters,
                                  T2dTsvParameters tsvParameters) {
        this.service = service;
        this.metadataParameters = metadataParameters;
        this.tsvParameters = tsvParameters;
    }

    public void storeTableDefinitionInContext(ChunkContext chunkContext, T2DTableStructure structure) {
        chunkContext.getStepContext().getStepExecution().getExecutionContext()
                .put(CONTEXT_TSV_DEFINITION, structure);
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        T2DTableStructure dataStructure = T2dTableStructureParser.getTableStructure(
                getTableName(metadataParameters, tsvParameters),
                getTableDefinitionFilePath(tsvParameters)
        );
        storeTableDefinitionInContext(chunkContext, dataStructure);
        service.createTable(dataStructure);
        insertProperties(service, getDatasetId(metadataParameters), dataStructure, tsvParameters);

        return RepeatStatus.FINISHED;
    }

    protected abstract void insertProperties(T2dService service, String datasetId, T2DTableStructure dataStructure,
                                             T2dTsvParameters tsvParameters);

    protected abstract String getDatasetId(T2dMetadataParameters metadataParameters);

    protected abstract String getTableDefinitionFilePath(T2dTsvParameters tsvParameters);

    protected abstract String getTableName(T2dMetadataParameters metadataParameters, T2dTsvParameters tsvParameters);

}
