package uk.ac.ebi.eva.t2d.jobs.tasklet;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.repeat.RepeatStatus;
import uk.ac.ebi.eva.t2d.model.T2DTableStructure;
import uk.ac.ebi.eva.t2d.model.T2dDataSourceAdaptor;
import uk.ac.ebi.eva.t2d.model.T2dDatasourceAdaptorAppender;
import uk.ac.ebi.eva.t2d.model.T2dDatasourceAdaptorReference;
import uk.ac.ebi.eva.t2d.model.T2dDatasourceAdaptorStaticString;
import uk.ac.ebi.eva.t2d.parameters.T2dMetadataParameters;
import uk.ac.ebi.eva.t2d.parameters.T2dTsvParameters;
import uk.ac.ebi.eva.t2d.services.T2dService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.Stream;

import static uk.ac.ebi.eva.t2d.parameters.T2dJobParametersNames.CONTEXT_TSV_DEFINITION;

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

    protected void parseLine(T2DTableStructure dataStructure, String line) {
        String[] columns = line.split("\t");

        checkNumberOfColumns(line, columns);
        dataStructure.put(columns[0], parseType(columns[1]), parseT2dDataSourceAdaptors(columns[2]));
    }

    private T2dDataSourceAdaptor parseT2dDataSourceAdaptors(String columns) {
        String[] sourceAdaptors = columns.split("\\+");
        if (sourceAdaptors.length == 1) {
            return parseT2dDataSourceAdaptor(sourceAdaptors[0]);
        } else {
            return new T2dDatasourceAdaptorAppender(Stream.of(sourceAdaptors).map(this::parseT2dDataSourceAdaptor));
        }

    }

    private T2dDataSourceAdaptor parseT2dDataSourceAdaptor(String sourceAdaptor) {
        if (sourceAdaptor.startsWith("$")) {
            return new T2dDatasourceAdaptorReference(sourceAdaptor);
        } else {
            if (!sourceAdaptor.startsWith("\"")) {
                throw new RuntimeException("Error parsing '" + sourceAdaptor + "' string doesn't start with \"");
            }
            if (!sourceAdaptor.endsWith("\"")) {
                throw new RuntimeException("Error parsing '" + sourceAdaptor + "' string doesn't end with \"");
            }
            return new T2dDatasourceAdaptorStaticString(sourceAdaptor.substring(1, sourceAdaptor.length() - 1));
        }
    }

    protected Class<?> parseType(String column) {
        switch (column) {
            case "integer":
            case "INTEGER":
                return Integer.class;
            case "float":
            case "FLOAT":
                return Float.class;
            case "double":
            case "DOUBLE":
                return Double.class;
            case "boolean":
            case "BOOLEAN":
                return Boolean.class;
            case "string":
            case "STRING":
                return String.class;
        }
        throw new ParseException("Type not known or supported '" + column + "'");
    }

    private void checkNumberOfColumns(String line, String[] columns) {
        if (columns.length != 3) {
            throw new ParseException("Syntax error in sample data structure file, invalid total columns '" +
                    line + "'");
        }
    }

    public T2DTableStructure readT2DTableStructure(String tableName) throws IOException {
        T2DTableStructure dataStructure = new T2DTableStructure(tableName);
        try (Stream<String> stream = Files.lines(new File(getTableDefinitionFilePath(tsvParameters)).toPath())) {
            stream.filter(line -> !line.isEmpty()).forEach(line -> parseLine(dataStructure, line));
        }
        return dataStructure;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        T2DTableStructure dataStructure = readT2DTableStructure(getTableName(metadataParameters, tsvParameters));
        storeTableDefinitionInContext(chunkContext, dataStructure);
        service.createTable(dataStructure);
        insertProperties(service, getDatasetId(metadataParameters), dataStructure, tsvParameters);

        return RepeatStatus.FINISHED;
    }

    protected abstract void insertProperties(T2dService service, String datasetId, T2DTableStructure dataStructure, T2dTsvParameters tsvParameters);

    protected abstract String getDatasetId(T2dMetadataParameters metadataParameters);

    protected abstract String getTableDefinitionFilePath(T2dTsvParameters tsvParameters);

    protected abstract String getTableName(T2dMetadataParameters metadataParameters, T2dTsvParameters tsvParameters);

}
