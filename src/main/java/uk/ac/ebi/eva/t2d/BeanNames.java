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
package uk.ac.ebi.eva.t2d;

public class BeanNames {

    public static final String T2D_LOAD_SAMPLES_DATA_JOB = "load-sample-data-job";
    public static final String T2D_LOAD_SUMMARY_STATISTICS_JOB = "load-summary-data-job";
    public static final String T2D_RELEASE_DATASET_JOB = "release-dataset-job";
    public static final String T2D_RELEASE_DATASET_GAIT_JOB = "release-dataset-gait-job";
    public static final String T2D_ANNOTATE_AND_LOAD_VCF_JOB = "annotate-and-load-vcf-job";
    public static final String T2D_LOAD_VEP_ANNOTATION_JOB = "load-vep-annotation-job";
    public static final String T2D_GENERATE_PED_JOB = "generate-ped-job";


    public static final String T2D_GENERATE_PED_STEP = "generate-ped-step";
    public static final String T2D_READ_SAMPLE_DEFINITION = "read-sample-definition";
    public static final String T2D_PREPARE_DATABASE_SAMPLES_STEP = "prepare-database-samples-step";
    public static final String T2D_LOAD_SAMPLES_DATA_STEP = "load-samples-data-step";
    public static final String T2D_PREPARE_DATABASE_SUMMARY_STATISTICS_STEP =
            "prepare-database-summary-statistics-step";
    public static final String T2D_LOAD_SUMMARY_STATISTICS_STEP = "load-summary-statistics-step";
    public static final String T2D_RELEASE_DATASET_STEP = "release-dataset-step";
    public static final String T2D_RELEASE_DATASET_GAIT_STEP = "release-dataset-gait-step";
    public static final String T2D_VEP_ANNOTATION_STEP = "t2d-vep-annotation-step";
    public static final String T2D_LOAD_ANNOTATION_STEP = "t2d-load-annotation-step";
    public static final String T2D_MANUAL_LOAD_ANNOTATION_STEP = "t2d-manual-load-annotation-step";

    public static final String T2D_SAMPLES_DATA_STRUCTURE_READER = "t2d-samples-data-structure-reader";
    public static final String T2D_SAMPLES_READER = "t2d-samples-reader";
    public static final String T2D_SUMMARY_STATISTICS_READER = "t2d-summary-statistics-reader";

    public static final String T2D_VARIANT_ANNOTATION_READER = "t2d-variant-annotation-reader";
    public static final String T2D_MANUAL_VARIANT_ANNOTATION_READER = "t2d-manual-variant-annotation-reader";
    public static final String T2D_TSV_WRITER = "t2d-tsv-writer";
    public static final String T2D_PED_WRITER = "t2d-ped-writer";
    public static final String T2D_ANNOTATION_LOAD_WRITER = "t2d-annotation-load-writer";

    public static final String T2D_TSV_PROCESSOR = "t2d-tsv-processor";

    public static final String EXISTING_VARIANT_FILTER = "t2d-existing-variant-filter";

}
