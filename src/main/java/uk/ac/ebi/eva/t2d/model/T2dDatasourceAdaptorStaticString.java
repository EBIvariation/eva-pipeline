package uk.ac.ebi.eva.t2d.model;

import java.util.Map;

public class T2dDatasourceAdaptorStaticString implements T2dDataSourceAdaptor {

    private final String staticString;

    public T2dDatasourceAdaptorStaticString(String sourceAdaptor) {
        this.staticString = sourceAdaptor;
    }

    @Override
    public String evaluate(Map<String, String> columnIdValue) {
        return staticString;
    }
}
