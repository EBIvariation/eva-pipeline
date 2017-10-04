package uk.ac.ebi.eva.t2d.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import uk.ac.ebi.eva.t2d.model.T2DTableStructure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SqlUtils {

    private static final Logger logger = LoggerFactory.getLogger(SqlUtils.class);

    private static final Map<Class<?>, String> parseTypeMap;

    static {
        Map<Class<?>, String> temp = new HashMap<>();
        temp.put(Boolean.class, "BOOLEAN DEFAULT NULL");
        temp.put(Integer.class, "INTEGER DEFAULT NULL");
        temp.put(Float.class, "FLOAT DEFAULT NULL");
        temp.put(Double.class, "DOUBLE DEFAULT NULL");
        temp.put(String.class, " TEXT DEFAULT NULL");
        parseTypeMap = Collections.unmodifiableMap(temp);
    }

    private static String toSqlFieldDeclaration(Map.Entry<String, Class<?>> entry) {
        return toSqlField(entry.getKey()) + parseTypeMap.get(entry.getValue());
    }

    private static String toSqlField(String field) {
        return "`" + field + "` ";
    }

    public static String sqlCreateTable(T2DTableStructure tableStructure) {
        String query = "CREATE TABLE IF NOT EXISTS " + toSqlField(tableStructure.getTableName()) + " ";
        query += par(sqlTableDefinition(tableStructure));
        logger.debug(query);
        return query;
    }

    private static String par(String text) {
        return "(" + text + ")";
    }

    private static String sqlTableDefinition(T2DTableStructure dataStructure) {
        String fields = toSqlFieldDeclarations(dataStructure.streamFields());
        if (!dataStructure.getPrimaryKeys().isEmpty()) {
            fields += SqlUtils.sqlPrimaryKeyDeclaration(dataStructure.getPrimaryKeys());
        }
        return fields;
    }

    private static String toSqlFieldDeclarations(Stream<Map.Entry<String, Class<?>>> entryStream) {
        return entryStream.map(SqlUtils::toSqlFieldDeclaration).collect(Collectors.joining(", "));
    }

    private static String sqlPrimaryKeyDeclaration(Collection<String> primaryKeys) {
        return ", PRIMARY KEY " + par(toSqlFieldList(primaryKeys));
    }

    private static String toSqlFieldList(Collection<String> keys) {
        return keys.stream().map(SqlUtils::toSqlField).collect(Collectors.joining(", "));
    }

    public static String sqlInsert(T2DTableStructure tableStructure, LinkedHashSet<String> fieldNames,
                                   List<? extends List<String>> data) {
        List<Class<?>> typesForFields = getFieldTypes(tableStructure, fieldNames);
        String query = "INSERT INTO " + toSqlField(tableStructure.getTableName()) + " ";
        query += par(toSqlFieldList(fieldNames));
        query += " VALUES ";
        query += toValuesArray(typesForFields, data);
        logger.debug(query);
        return query;
    }

    private static List<Class<?>> getFieldTypes(T2DTableStructure tableStructure, LinkedHashSet<String> fieldNames) {
        return fieldNames.stream().map(tableStructure::getFieldType).collect(Collectors.toList());
    }

    private static String toValuesArray(List<Class<?>> typesForFields, List<? extends List<String>> data) {
        //Both lists have the same length check already done in public function
        return data.stream()
                .map(values -> toValues(typesForFields, values))
                .collect(Collectors.joining(","));
    }

    private static String toValues(List<Class<?>> typesForFields, List<String> values) {
        Assert.isTrue(typesForFields.size() == values.size(), "List of values and colums have different sizes.");
        List<String> convertedValues = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            convertedValues.add(nullOrValue(values.get(i), typesForFields.get(i)));
        }
        return par(String.join(",", convertedValues));
    }

    private static String nullOrValue(String value, Class<?> aClass) {
        if (value == null) {
            return "NULL";
        }
        String trimmedValue = value.trim();
        if (trimmedValue.isEmpty()) {
            return "NULL";
        }
        if (Objects.equals(String.class, aClass)) {
            return "'" + trimmedValue + "'";
        } else {
            return value;
        }
    }
}
