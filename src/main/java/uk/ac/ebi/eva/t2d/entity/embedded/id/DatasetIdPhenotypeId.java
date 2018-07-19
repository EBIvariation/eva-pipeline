/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.t2d.entity.embedded.id;

import uk.ac.ebi.eva.t2d.entity.DatasetMetadata;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

/**
 * Composed DATASET / PHENOTYPE
 */
@Embeddable
public class DatasetIdPhenotypeId implements Serializable {

    @Column(name = "DATASET")
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
