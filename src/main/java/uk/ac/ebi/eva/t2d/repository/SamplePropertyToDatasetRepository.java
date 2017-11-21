/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.t2d.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ebi.eva.t2d.entity.SamplePropertyToDataset;
import uk.ac.ebi.eva.t2d.model.T2DTableStructure;

@Repository
@Transactional
public interface SamplePropertyToDatasetRepository extends CrudRepository<SamplePropertyToDataset, String> {

    default void save(String datasetId, T2DTableStructure structure) {
        save(generate(datasetId, structure));
    }

    // TODO promove to private on java 9
    default Iterable<SamplePropertyToDataset> generate(String datasetId, T2DTableStructure structure) {
        return structure.getOrderedFieldIdSet().stream()
                .map(field -> new SamplePropertyToDataset(datasetId, field))
                ::iterator;
    }

}
