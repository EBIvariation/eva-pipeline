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
package uk.ac.ebi.eva.t2d.entity;

import uk.ac.ebi.eva.t2d.entity.embedded.id.DatasetIdToPhenotypeId;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * JPA definition for the table with the relationship between DATASET ID and PHENOTYPES
 */
@Entity
@Table(name = "META_ID_PH")
public class DatasetIdToPhenotype {

    @Id
    private DatasetIdToPhenotypeId datasetIdToPhenotypeId;

    DatasetIdToPhenotype() {
    }

    public DatasetIdToPhenotypeId getDatasetIdToPhenotypeId() {
        return datasetIdToPhenotypeId;
    }
}
