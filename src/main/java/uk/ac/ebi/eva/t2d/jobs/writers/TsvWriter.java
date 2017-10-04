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

import java.util.LinkedHashSet;
import java.util.List;

import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.CONTEXT_TSV_DEFINITION;
import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.CONTEXT_TSV_HEADER;

public class TsvWriter implements ItemStreamWriter<List<String>> {

    private final T2dService service;

    private T2DTableStructure tableStructure;

    private LinkedHashSet<String> fieldNames;

    public TsvWriter(T2dService service) {
        this.service = service;
    }

    @Override
    public void write(List<? extends List<String>> items) throws Exception {
        service.insertData(tableStructure, fieldNames, items);
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        // This data comes from the reader, it is found in the step context
        fieldNames = (LinkedHashSet<String>) executionContext.get(CONTEXT_TSV_HEADER);
        Assert.notNull(fieldNames, "Could not get samples file header from step context");
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
