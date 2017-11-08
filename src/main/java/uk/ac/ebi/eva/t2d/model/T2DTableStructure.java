package uk.ac.ebi.eva.t2d.model;

import uk.ac.ebi.eva.t2d.model.exceptions.FieldDoesNotExistException;
import uk.ac.ebi.eva.t2d.model.exceptions.FieldTypeIsNotValidAsPrimaryKeyException;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class T2DTableStructure implements Serializable {

    private final String tableName;

    private final LinkedHashMap<String, T2dColumnDefinition> fieldMap;

    private final Set<String> primaryKeys;

    private final Set<String> indexes;

    public T2DTableStructure(String tableName) {
        this.tableName = tableName;
        fieldMap = new LinkedHashMap<>();
        primaryKeys = new HashSet<>();
        indexes = new HashSet<>();
    }

    public void put(String fieldName, Class<?> aClass, T2dDataSourceAdaptor adaptor) {
        fieldMap.put(fieldName, new T2dColumnDefinition(aClass, adaptor));
    }

    public void setPrimaryKeys(String fieldName) throws FieldDoesNotExistException,
            FieldTypeIsNotValidAsPrimaryKeyException {
        Class<?> clazz = checkedGetField(fieldName).getType();
        if (clazz != Integer.class && clazz != String.class) {
            throw new FieldTypeIsNotValidAsPrimaryKeyException();
        }
        primaryKeys.add(fieldName);
    }

    private T2dColumnDefinition checkedGetField(String fieldName) throws FieldDoesNotExistException {
        T2dColumnDefinition definition = fieldMap.get(fieldName);
        if (definition == null) {
            throw new FieldDoesNotExistException();
        }
        return definition;
    }

    public void addIndex(String fieldName) throws FieldDoesNotExistException {
        checkedGetField(fieldName);
        if (!primaryKeys.contains(fieldName)) {
            indexes.add(fieldName);
        }
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

    public Set<String> getOrderedFieldIdSet() {
        return fieldMap.keySet();
    }

    public Set<Map.Entry<String, T2dColumnDefinition>> getOrderedColumnIdAndDefinition() {
        return fieldMap.entrySet();
    }

    public Collection<T2dColumnDefinition> getOrderedDefinitions() {
        return fieldMap.values();
    }

    public Class<?> getFieldType(String columnId) {
        return fieldMap.get(columnId).getType();
    }

    public List<Class<?>> getFieldTypes() {
        return fieldMap.values().stream().map(T2dColumnDefinition::getType).collect(Collectors.toList());
    }
}
