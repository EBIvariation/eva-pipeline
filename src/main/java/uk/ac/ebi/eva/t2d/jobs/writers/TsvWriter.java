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
package uk.ac.ebi.eva.t2d.jobs.writers;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.util.Assert;
import uk.ac.ebi.eva.t2d.model.T2DTableStructure;
import uk.ac.ebi.eva.t2d.services.T2dService;

import java.util.List;

import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.CONTEXT_TSV_DEFINITION;

/**
 * Writer that takes the mapping information and the transformed data and inserts the record on the database
 */
public class TsvWriter implements ItemStreamWriter<List<String>> {

    private final T2dService service;

    private T2DTableStructure tableStructure;

    public TsvWriter(T2dService service) {
        this.service = service;
    }

    @Override
    public void write(List<? extends List<String>> items) throws Exception {
        service.insertData(tableStructure, items);
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        // Do nothing
    }

    @BeforeStep
    public void retrieveSharedStepDate(StepExecution stepExecution) {
        // This data comes from another step, data is in the job context
        JobExecution jobExecution = stepExecution.getJobExecution();
        tableStructure = (T2DTableStructure) jobExecution.getExecutionContext().get(CONTEXT_TSV_DEFINITION);
        Assert.notNull(tableStructure, "Could not get table structure from job context");
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        // Do nothing the execution context should not be updated.
    }

    @Override
    public void close() throws ItemStreamException {
        // Do nothing, service handles this.
    }
}
