package uk.ac.ebi.eva.t2d.model;

import java.util.Map;

public class T2dDatasourceAdaptorReference implements T2dDataSourceAdaptor {

    private final String columnId;

    public T2dDatasourceAdaptorReference(String sourceAdaptor) {
        columnId = sourceAdaptor.substring(1);
    }

    @Override
    public String evaluate(Map<String, String> columnIdValue) {
        return columnIdValue.get(columnId);
    }
}
