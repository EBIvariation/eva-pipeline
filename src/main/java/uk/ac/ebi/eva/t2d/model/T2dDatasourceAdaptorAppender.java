package uk.ac.ebi.eva.t2d.model;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class T2dDatasourceAdaptorAppender implements T2dDataSourceAdaptor {

    private final List<T2dDataSourceAdaptor> adaptors;

    public T2dDatasourceAdaptorAppender(Stream<T2dDataSourceAdaptor> t2dDataSourceAdaptorStream) {
        this.adaptors = t2dDataSourceAdaptorStream.collect(Collectors.toList());
    }

    @Override
    public String evaluate(Map<String, String> columnIdValue) {
        return adaptors.stream().map(adaptor -> adaptor.evaluate(columnIdValue)).collect(Collectors.joining());
    }
}
