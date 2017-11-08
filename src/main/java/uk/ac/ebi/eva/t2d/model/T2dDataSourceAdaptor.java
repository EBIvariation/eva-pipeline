package uk.ac.ebi.eva.t2d.model;

import java.io.Serializable;
import java.util.Map;

public interface T2dDataSourceAdaptor extends Serializable {

    String evaluate(Map<String, String> columnIdValue);

}
