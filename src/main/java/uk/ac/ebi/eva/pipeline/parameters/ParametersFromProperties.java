/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.lang.reflect.Field;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class resolves all parameters to be used in the jobs from the application context
 * (They can be loaded from properties, environmental values...) and can create a
 * @see org.springframework.batch.core.JobParameters
 */
@Service
public class ParametersFromProperties {

    private static final Logger logger = LoggerFactory.getLogger(ParametersFromProperties.class);
    private static final String PROPERTY = "${";
    private static final String OR_NULL = ":#{null}}";
    private static final String PROPERTY_ID_REGEX = "(?<=\\$\\{).*(?=:#\\{null})";

    @Value(PROPERTY + JobParametersNames.DB_NAME + OR_NULL)
    private String databaseName;

    @Value(PROPERTY + JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME + OR_NULL)
    private String collectionVariantsName;

    @Value(PROPERTY + JobParametersNames.DB_COLLECTIONS_FILES_NAME + OR_NULL)
    private String collectionFilesName;

    @Value(PROPERTY + JobParametersNames.INPUT_STUDY_ID + OR_NULL)
    private String studyId;

    @Value(PROPERTY + JobParametersNames.INPUT_VCF_ID + OR_NULL)
    private String vcfId;

    @Value(PROPERTY + JobParametersNames.INPUT_VCF + OR_NULL)
    private String vcf;

    @Value(PROPERTY + JobParametersNames.INPUT_VCF_AGGREGATION + OR_NULL)
    private String vcfAggregation;

    @Value(PROPERTY + JobParametersNames.OUTPUT_DIR_STATISTICS + OR_NULL)
    private String outputDirStats;

    public Properties getProperties() {
        Properties properties = new Properties();

        for (Field field : getClass().getDeclaredFields()) {
            try {
                if (field.isAnnotationPresent(Value.class)) {
                    Value value = field.getAnnotation(Value.class);
                    String fieldName = value.value();
                    Matcher matcher = Pattern.compile(PROPERTY_ID_REGEX).matcher(fieldName);
                    matcher.find();
                    String propertyName = matcher.group(0);
                    if (field.get(this) != null) {
                        properties.put(propertyName, field.get(this));
                    }
                }
            } catch (IllegalAccessException e) {
                logger.debug("Value retrieval error", e);
            }
        }
        return properties;
    }

    public String getDatabaseName() {
        return databaseName;
    }
}
