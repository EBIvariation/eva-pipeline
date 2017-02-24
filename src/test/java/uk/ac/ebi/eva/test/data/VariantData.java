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

    private static final String VARIANT_WITHOUT_ANNOTATION_PATH = "/input-files/annotation/VariantWithOutAnnotation";
    private static final String VARIANT_WITH_ANNOTATION_PATH = "/input-files/annotation/VariantWithAnnotation";
    private static final String POPULATION_STATS_PATH = "/input-files/statistics/PopulationStatistics.json";
    private static final String VARIANTS_WITH_ONE_STUDY_TO_DROP_PATH = "/input-files/vcf/VariantWithOneStudy.json";
    private static final String VARIANTS_WITH_TWO_STUDIES_TO_DROP_PATH = "/input-files/vcf/VariantWithTwoStudies.json";

    public static String getVariantWithoutAnnotation() throws IOException {
        return FileUtils.readFileToString(getResource(VARIANT_WITHOUT_ANNOTATION_PATH));
    }

    public static String getVariantWithAnnotation() throws IOException {
        return FileUtils.readFileToString(getResource(VARIANT_WITH_ANNOTATION_PATH));
    }

    public static String getPopulationStatistics() throws IOException {
        return FileUtils.readFileToString(getResource(POPULATION_STATS_PATH));
    }

    public static String getVariantsWithOneStudyToDrop() throws IOException {
        return FileUtils.readFileToString(TestFileUtils.getResource(VARIANTS_WITH_ONE_STUDY_TO_DROP_PATH));
    }

    public static String getVariantsWithTwoStudiesToDrop() throws IOException {
        return FileUtils.readFileToString(TestFileUtils.getResource(VARIANTS_WITH_TWO_STUDIES_TO_DROP_PATH));
    }
}
