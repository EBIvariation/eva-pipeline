package uk.ac.ebi.eva.t2d.entity;

import uk.ac.ebi.eva.t2d.entity.embedded.id.PropertyIdDatasetId;

import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "SAMPLES_PROP_ID")
public class SamplePropertyToDataset {


    @EmbeddedId
    private PropertyIdDatasetId id;

    SamplePropertyToDataset() {
    }

    public SamplePropertyToDataset(String datasetId, String propertyId) {
        id = new PropertyIdDatasetId(propertyId, datasetId);
    }

    public String getDatasetId() {
        return id.getDatasetId();
    }
}
