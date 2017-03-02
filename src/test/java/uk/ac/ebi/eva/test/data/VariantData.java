/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.test.data;

import org.apache.commons.io.FileUtils;

import java.io.IOException;

import static uk.ac.ebi.eva.utils.FileUtils.getResource;

public class VariantData {

    private static final String VARIANT_WITHOUT_ANNOTATION_PATH = "/input-files/annotation/VariantWithOutAnnotation.json";
    private static final String VARIANT_WITHOUT_ANNOTATION_OTHER_STUDY_PATH = "/input-files/annotation/VariantWithOutAnnotationOtherStudy.json";
    private static final String VARIANT_WITH_ANNOTATION_PATH = "/input-files/annotation/VariantWithAnnotation.json";
    private static final String POPULATION_STATS_PATH = "/input-files/statistics/PopulationStatistics.json";
    private static final String VARIANT_WITH_ONE_STUDY_TO_DROP_PATH = "/input-files/variant/VariantWithOneStudyToDrop.json";
    private static final String OTHER_VARIANT_WITH_ONE_STUDY_TO_DROP_PATH = "/input-files/variant/OtherVariantWithOneStudyToDrop.json";
    private static final String VARIANT_WITH_ONE_STUDY_PATH = "/input-files/variant/VariantWithOneStudy.json";
    private static final String VARIANT_WITH_TWO_STUDIES_PATH = "/input-files/variant/VariantWithTwoStudies.json";

    public static String getVariantWithoutAnnotationOtherStudy() throws IOException {
        return FileUtils.readFileToString(getResource(VARIANT_WITHOUT_ANNOTATION_OTHER_STUDY_PATH));
    }

    public static String getVariantWithoutAnnotation() throws IOException {
        return FileUtils.readFileToString(getResource(VARIANT_WITHOUT_ANNOTATION_PATH));
    }

    public static String getVariantWithAnnotation() throws IOException {
        return FileUtils.readFileToString(getResource(VARIANT_WITH_ANNOTATION_PATH));
    }

    public static String getPopulationStatistics() throws IOException {
        return FileUtils.readFileToString(getResource(POPULATION_STATS_PATH));
    }

    public static String getVariantWithOneStudyToDrop() throws IOException {
        return FileUtils.readFileToString(getResource(VARIANT_WITH_ONE_STUDY_TO_DROP_PATH));
    }

    public static String getOtherVariantWithOneStudyToDrop() throws IOException {
        return FileUtils.readFileToString(getResource(OTHER_VARIANT_WITH_ONE_STUDY_TO_DROP_PATH));
    }

    public static String getVariantWithOneStudy() throws IOException {
        return FileUtils.readFileToString(getResource(VARIANT_WITH_ONE_STUDY_PATH));
    }

    public static String getVariantWithTwoStudies() throws IOException {
        return FileUtils.readFileToString(getResource(VARIANT_WITH_TWO_STUDIES_PATH));
    }
}
