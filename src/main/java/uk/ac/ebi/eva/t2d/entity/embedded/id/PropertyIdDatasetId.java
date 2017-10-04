/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.t2d.entity.embedded.id;

import uk.ac.ebi.eva.t2d.entity.DatasetMetadata;
import uk.ac.ebi.eva.t2d.entity.SamplesDatasetMetadata;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Embeddable
public class PropertyIdDatasetId implements Serializable {

    @Column(name = "PROP")
    private String propertyId;

    @Column(name = "ID")
    private String datasetId;

    PropertyIdDatasetId() {
    }

    public PropertyIdDatasetId(String propertyId, String datasetId){
        this.propertyId = propertyId;
        this.datasetId = datasetId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PropertyIdDatasetId)) return false;

        PropertyIdDatasetId that = (PropertyIdDatasetId) o;

        if (!propertyId.equals(that.propertyId)) return false;
        return datasetId.equals(that.datasetId);

    }

    public String getDatasetId() {
        return datasetId;
    }

    @Override
    public int hashCode() {
        int result = propertyId.hashCode();
        result = 31 * result + datasetId.hashCode();
        return result;
    }
}
