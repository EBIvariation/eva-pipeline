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
package uk.ac.ebi.eva.pipeline.parameters;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Service that holds access to the values for database. This include the configuration
 * values for database connection that are got as values not parameters.
 */
@Service
@StepScope
public class DatabaseParameters {

    private static final String PARAMETER = "#{jobParameters['";
    private static final String END = "']}";

    @Value(PARAMETER + JobParametersNames.DB_NAME + END)
    private String databaseName;

    @Value(PARAMETER + JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME + END)
    private String collectionVariantsName;

    @Value(PARAMETER + JobParametersNames.DB_COLLECTIONS_FILES_NAME + END)
    private String collectionFilesName;

    @Value(PARAMETER + JobParametersNames.DB_COLLECTIONS_FEATURES_NAME + END)
    private String collectionFeaturesName;

    @Value(PARAMETER + JobParametersNames.DB_COLLECTIONS_ANNOTATION_METADATA_NAME + END)
    private String collectionAnnotationMetadataName;

    @Value(PARAMETER + JobParametersNames.DB_COLLECTIONS_ANNOTATIONS_NAME + END)
    private String collectionAnnotationsName;

    @Autowired
    private MongoConnection mongoConnection;

    public MongoConnection getMongoConnection() {
        return mongoConnection;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getCollectionVariantsName() {
        return collectionVariantsName;
    }

    public String getCollectionFilesName() {
        return collectionFilesName;
    }

    public String getCollectionFeaturesName() {
        return collectionFeaturesName;
    }

    public String getCollectionAnnotationMetadataName() {
        return collectionAnnotationMetadataName;
    }

    public String getCollectionAnnotationsName() {
        return collectionAnnotationsName;
    }
}
