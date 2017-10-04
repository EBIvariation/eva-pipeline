package uk.ac.ebi.eva.t2d.entity.embedded.id;

import uk.ac.ebi.eva.t2d.entity.DatasetMetadata;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Embeddable
public class DatasetIdPhenotypeId implements Serializable {

    @Column(name = "ID")
    private String datasetId;

    @Column(name = "PH")
    private String phenotypeId;

    DatasetIdPhenotypeId() {
    }

    public DatasetIdPhenotypeId(DatasetMetadata datasetMetadata, String phenotypeId) {
        this.datasetId = datasetMetadata.getId();
        this.phenotypeId = phenotypeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DatasetIdPhenotypeId)) return false;

        DatasetIdPhenotypeId that = (DatasetIdPhenotypeId) o;

        if (!datasetId.equals(that.datasetId)) return false;
        return phenotypeId.equals(that.phenotypeId);

    }

    @Override
    public int hashCode() {
        int result = datasetId.hashCode();
        result = 31 * result + phenotypeId.hashCode();
        return result;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public String getPhenotypeId() {
        return phenotypeId;
    }
}
