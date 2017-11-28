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
package uk.ac.ebi.eva.utils;

import org.springframework.batch.item.ParseException;
import uk.ac.ebi.eva.t2d.model.T2DTableStructure;
import uk.ac.ebi.eva.t2d.model.T2dDataSourceAdaptor;
import uk.ac.ebi.eva.t2d.model.T2dDatasourceAdaptorAppender;
import uk.ac.ebi.eva.t2d.model.T2dDatasourceAdaptorReference;
import uk.ac.ebi.eva.t2d.model.T2dDatasourceAdaptorStaticString;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.Stream;

public class T2dTableStructureParser {

    public static T2DTableStructure getTableStructure(String tableName, String definitionFilePath) throws IOException {
        T2DTableStructure dataStructure = new T2DTableStructure(tableName);
        try (Stream<String> stream = Files.lines(new File(definitionFilePath).toPath())) {
            stream.filter(line -> !line.isEmpty()).forEach(line -> parseLine(dataStructure, line));
        }
        return dataStructure;
    }

    private static void parseLine(T2DTableStructure dataStructure, String line) {
        String[] columns = line.split("\t");
        checkNumberOfColumns(line, columns);
        dataStructure.put(columns[0], parseType(columns[1]), parseT2dDataSourceAdaptors(columns[2]));
    }

    private static void checkNumberOfColumns(String line, String[] columns) {
        if (columns.length != 3) {
            throw new ParseException("Syntax error in sample data structure file, invalid total columns '" +
                    line + "'");
        }
    }

    private static T2dDataSourceAdaptor parseT2dDataSourceAdaptors(String columns) {
        String[] sourceAdaptors = columns.split("\\+");
        if (sourceAdaptors.length == 1) {
            return parseT2dDataSourceAdaptor(sourceAdaptors[0]);
        } else {
            return new T2dDatasourceAdaptorAppender(
                    Stream.of(sourceAdaptors).map(T2dTableStructureParser::parseT2dDataSourceAdaptors));
        }

    }

    private static T2dDataSourceAdaptor parseT2dDataSourceAdaptor(String sourceAdaptor) {
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

    private static Class<?> parseType(String column) {
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

}
