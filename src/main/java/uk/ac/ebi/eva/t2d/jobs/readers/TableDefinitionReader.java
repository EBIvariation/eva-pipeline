package uk.ac.ebi.eva.t2d.jobs.readers;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import uk.ac.ebi.eva.t2d.model.T2DTableStructure;

import java.io.File;
import java.nio.file.Files;
import java.util.stream.Stream;

public class TableDefinitionReader implements ItemReader<T2DTableStructure> {

    private final File filename;

    private final String tableName;

    public TableDefinitionReader(File file, String tableName) {
        this.filename = file;
        this.tableName = tableName;
    }

    @Override
    public T2DTableStructure read() throws Exception, UnexpectedInputException, ParseException,
            NonTransientResourceException {
        try (Stream<String> stream = Files.lines(filename.toPath())) {
            T2DTableStructure dataStructure = new T2DTableStructure(tableName);
            stream.forEach(line -> parseLine(dataStructure, line));
            return dataStructure;
        }
    }

    private void parseLine(T2DTableStructure dataStructure, String line) {
        String[] columns = line.split("\t");
        checkNumberOfColumns(line, columns);
        dataStructure.put(columns[0], parseType(columns[1]));
    }

    private Class<?> parseType(String column) {
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
        if (columns.length != 2) {
            throw new ParseException("Syntax error in sample data structure file, invalid total columns '" +
                    line + "'");
        }
    }
}
