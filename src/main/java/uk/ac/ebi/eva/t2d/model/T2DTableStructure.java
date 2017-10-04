package uk.ac.ebi.eva.t2d.model;

import uk.ac.ebi.eva.t2d.model.exceptions.FieldDoesNotExistException;
import uk.ac.ebi.eva.t2d.model.exceptions.FieldTypeIsNotValidAsPrimaryKeyException;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class T2DTableStructure implements Serializable {

    private final String tableName;

    private final Map<String, Class<?>> fieldMap;

    private final Set<String> primaryKeys;

    private final Set<String> indexes;

    public T2DTableStructure(String tableName) {
        this.tableName = tableName;
        fieldMap = new HashMap<>();
        primaryKeys = new HashSet<>();
        indexes = new HashSet<>();
    }

    public void put(String fieldName, Class<?> aClass) {
        fieldMap.put(fieldName, aClass);
    }

    public void setPrimaryKeys(String fieldName) throws FieldDoesNotExistException,
            FieldTypeIsNotValidAsPrimaryKeyException {
        Class<?> clazz = checkedGetField(fieldName);
        if (clazz != Integer.class && clazz != String.class) {
            throw new FieldTypeIsNotValidAsPrimaryKeyException();
        }
        primaryKeys.add(fieldName);
    }

    private Class<?> checkedGetField(String fieldName) throws FieldDoesNotExistException {
        Class<?> clazz = fieldMap.get(fieldName);
        if (clazz == null) {
            throw new FieldDoesNotExistException();
        }
        return clazz;
    }

    public void addIndex(String fieldName) throws FieldDoesNotExistException {
        checkedGetField(fieldName);
        if (!primaryKeys.contains(fieldName)) {
            indexes.add(fieldName);
        }
    }

    public Stream<Map.Entry<String, Class<?>>> streamFields() {
        return fieldMap.entrySet().stream();
    }

    public Set<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public Set<String> getIndexes() {
        return indexes;
    }

    public String getTableName() {
        return tableName;
    }

    public Set<Map.Entry<String, Class<?>>> getFields() {
        return fieldMap.entrySet();
    }

    public Class<?> getFieldType(String name){
        return fieldMap.get(name);
    }
}
