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

package uk.ac.ebi.eva.pipeline.configuration;

/**
 * This class contains the name definition for the beans that are injected through the pipeline.
 */
public class BeanNames {

    public static final String GENE_READER = "gene-reader";
    public static final String VARIANTS_READER = "variants-reader";
    public static final String VARIANT_ANNOTATION_READER = "variant-annotation-reader";
    public static final String VARIANT_READER = "variant-reader";

    public static final String VEP_ANNOTATION_PROCESSOR = "vep-annotation-processor";
    public static final String ANNOTATION_PARSER_PROCESSOR = "annotation-parser-processor";
    public static final String ANNOTATION_COMPOSITE_PROCESSOR = "annotation-composite-processor";

    public static final String GENE_WRITER = "gene-writer";
    public static final String ANNOTATION_WRITER = "annotation-writer";
    public static final String ANNOTATION_IN_VARIANT_WRITER = "annotation-in-variant-writer";
    public static final String COMPOSITE_ANNOTATION_VARIANT_WRITER = "composite-annotation-variant-writer";
    public static final String VARIANT_WRITER = "variant-writer";

    public static final String ANNOTATION_SKIP_STEP_DECIDER = "annotation-skip-step-decider";
    public static final String STATISTICS_SKIP_STEP_DECIDER = "statistics-skip-step-decider";

    public static final String VEP_ANNOTATION_FLOW = "vep-annotation-flow";
    public static final String VEP_ANNOTATION_OPTIONAL_FLOW = "vep-annotation-optional.flow";
    public static final String PARALLEL_STATISTICS_AND_ANNOTATION = "parallel-statistics-and-annotation-flow";
    public static final String CALCULATE_STATISTICS_FLOW = "calculate-statistics-flow";
    public static final String CALCULATE_STATISTICS_OPTIONAL_FLOW = "calculate-statistics-optional-flow";

    public static final String LOAD_VEP_ANNOTATION_STEP = "load-vep-annotation-step";
    public static final String CALCULATE_STATISTICS_STEP = "calculate-statistics-step";
    public static final String CREATE_DATABASE_INDEXES_STEP = "create-database-indexes-step";
    public static final String LOAD_GENES_STEP = "load-genes-step";
    public static final String GENERATE_VEP_ANNOTATION_STEP = "generate-vep-annotation";
    public static final String LOAD_STATISTICS_STEP = "load-statistics-step";
    public static final String LOAD_VARIANTS_STEP = "load-variants-step";
    public static final String LOAD_FILE_STEP = "load-file-step";
    public static final String DROP_VARIANTS_BY_STUDY_STEP = "drop-variants-by-study-step";
    public static final String PULL_FILES_AND_STATISTICS_BY_STUDY_STEP = "pull-files-and-statistics-by-study-step";
    public static final String DROP_FILES_BY_STUDY_STEP = "drop-files-by-study-step";
    public static final String LOAD_ANNOTATION_METADATA_STEP = "annotation-metadata-step";

    public static final String AGGREGATED_VCF_JOB = "aggregated-vcf-job";
    public static final String ANNOTATE_VARIANTS_JOB = "annotate-variants-job";
    public static final String INIT_DATABASE_JOB = "init-database-job";
    public static final String GENOTYPED_VCF_JOB = "genotyped-vcf-job";
    public static final String CALCULATE_STATISTICS_JOB = "calculate-statistics-job";
    public static final String DROP_STUDY_JOB = "drop-study-job";
}
