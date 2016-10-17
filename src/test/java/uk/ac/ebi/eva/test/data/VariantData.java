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

import uk.ac.ebi.eva.pipeline.jobs.AnnotationJobTest;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class VariantData {

    private static final String VARIANT_WITHOUT_ANNOTATION_PATH = "/annotation/VariantWithOutAnnotation";
    private static final String VARIANT_WITH_ANNOTATION_PATH = "/annotation/VariantWithAnnotation";
    private static final String POPULATION_STATS_PATH = "/statistics/PopulationStatistics.json";

    public static String getVariantWithoutAnnotation() throws IOException {
        URL variantWithNoAnnotationUrl = VariantData.class.getResource(VARIANT_WITHOUT_ANNOTATION_PATH);
        return FileUtils.readFileToString(new File(variantWithNoAnnotationUrl.getFile()));
    }


    public static String getVariantWithAnnotation() throws IOException {
        URL variantWithAnnotationUrl = AnnotationJobTest.class.getResource(VARIANT_WITH_ANNOTATION_PATH);
        return FileUtils.readFileToString(new File(variantWithAnnotationUrl.getFile()));
    }

    public static String getPopulationStatsPath() throws IOException {
        URL stats = AnnotationJobTest.class.getResource(POPULATION_STATS_PATH);
        return FileUtils.readFileToString(new File(stats.getFile()));
    }
}
