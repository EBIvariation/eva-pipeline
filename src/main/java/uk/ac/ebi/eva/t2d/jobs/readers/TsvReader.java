package uk.ac.ebi.eva.t2d.jobs.readers;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineCallbackHandler;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.util.Assert;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.CONTEXT_TSV_HEADER;

public class TsvReader extends FlatFileItemReader<List<String>> {

    private StepExecution stepExecution;

    public TsvReader() {
        super();
        setLinesToSkip(1);
        setLineMapper(new LineMapper<List<String>>() {

            @Override
            public List<String> mapLine(String line, int lineNumber) throws Exception {
                return Arrays.asList(line.split("\t"));
            }

        });
        setSkippedLinesCallback(new LineCallbackHandler() {

            @Override
            public void handleLine(String line) {
                List<String> columnsInFile = Arrays.asList(line.split("\t"));
                LinkedHashSet<String> uniqueColumnsInFile = new LinkedHashSet<>(columnsInFile);
                Assert.isTrue(columnsInFile.size() == uniqueColumnsInFile.size(),
                        "Sample file contains duplicated columns");
                stepExecution.getExecutionContext().put(CONTEXT_TSV_HEADER, uniqueColumnsInFile);
            }

        });
    }

    @BeforeStep
    public void initializeStepExecution(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }
}
