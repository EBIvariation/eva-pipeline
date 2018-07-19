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
package uk.ac.ebi.eva.t2d.services;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.t2d.entity.DatasetMetadata;
import uk.ac.ebi.eva.t2d.entity.DatasetVersionMetadata;
import uk.ac.ebi.eva.t2d.entity.Phenotype;
import uk.ac.ebi.eva.t2d.entity.SamplesDatasetMetadata;
import uk.ac.ebi.eva.t2d.model.T2DTableStructure;
import uk.ac.ebi.eva.t2d.model.T2dAnnotation;

import java.util.List;

import static uk.ac.ebi.eva.t2d.configuration.T2dDataSourceConfiguration.T2D_TRANSACTION_MANAGER;

public interface T2dService {

    @Modifying
    @Transactional(T2D_TRANSACTION_MANAGER)
    void publishDataset(DatasetMetadata datasetMetadata, SamplesDatasetMetadata metadata, DatasetVersionMetadata datasetVersionMetadata);

    @Modifying
    @Transactional(T2D_TRANSACTION_MANAGER)
    void createTable(T2DTableStructure tableStructure);

    @Modifying
    @Transactional(T2D_TRANSACTION_MANAGER)
    void insertSampleProperties(String datasetId, T2DTableStructure structure);

    @Modifying
    @Transactional(T2D_TRANSACTION_MANAGER)
    void insertSampleProperties(String datasetId , T2DTableStructure structure, Phenotype phenotype);

    @Modifying
    @Transactional(T2D_TRANSACTION_MANAGER)
    void insertData(T2DTableStructure tableName, List<? extends List<String>> data);

    boolean exists(Variant variant);

    @Modifying
    @Transactional(T2D_TRANSACTION_MANAGER)
    void saveAnnotations(List<? extends T2dAnnotation> annotations);

    @Modifying
    @Transactional(T2D_TRANSACTION_MANAGER)
    void insertSample(T2DTableStructure tableStructure, List<? extends List<String>> items);
}
