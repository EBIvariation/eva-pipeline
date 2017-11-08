package uk.ac.ebi.eva.t2d.jobs.readers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineCallbackHandler;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.util.Assert;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TsvReader extends FlatFileItemReader<Map<String, String>> {

    private StepExecution stepExecution;

    private List<String> columns;

    private Set<String> nullValues;

    public TsvReader() {
        super();
        setLinesToSkip(1);
        setLineMapper(new LineMapper<Map<String, String>>() {

            @Override
            public Map<String, String> mapLine(String line, int lineNumber) throws Exception {
                HashMap<String, String> columnValues = new HashMap<>();
                // Split has the nasty functionality unless specified with a specific size or a negative one removes
                // trailing empty strings
                String[] values = line.split("\t",-1);
                Assert.isTrue(values.length == columns.size(),
                        "Line '" + lineNumber + "': Number of columns is different than header of the file. " +
                                "(header: " + columns.size() +
                                " row: " + values.length);

                for (int i = 0; i < values.length; i++) {
                    String value = values[i];
                    if (!nullValues.contains(value)) {
                        columnValues.put(columns.get(i), values[i]);
                    } else {
                        columnValues.put(columns.get(i), null);
                    }
                }
                return columnValues;
            }

        });
        setSkippedLinesCallback(new LineCallbackHandler() {

            @Override
            public void handleLine(String line) {
                // Split has the nasty functionality unless specified with a specific size or a negative one removes
                // trailing empty strings
                columns = Arrays.asList(line.split("\t",-1));
                LinkedHashSet<String> uniqueColumnsInFile = new LinkedHashSet<>(columns);
                Assert.isTrue(columns.size() == uniqueColumnsInFile.size(),
                        "Sample file contains duplicated columns");
            }

        });
    }

    @BeforeStep
    public void initializeStepExecution(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
        nullValues = new HashSet<>(Arrays.asList(new String[]{"NA", "NULL", "NIL", "", "nan", "NaN", "NAN", "-nan",
                "-NaN", "-NAN"}));
    }
}
