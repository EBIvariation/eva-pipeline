package uk.ac.ebi.eva.t2d.model;

import java.io.Serializable;

public class T2dColumnDefinition implements Serializable{

    private final Class<?> type;

    private final T2dDataSourceAdaptor adaptor;

    public T2dColumnDefinition(Class<?> type, T2dDataSourceAdaptor adaptor) {
        this.type = type;
        this.adaptor = adaptor;
    }

    public Class<?> getType() {
        return type;
    }

    public T2dDataSourceAdaptor getAdaptor() {
        return adaptor;
    }
}
