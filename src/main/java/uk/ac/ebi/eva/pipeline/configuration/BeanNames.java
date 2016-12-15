package uk.ac.ebi.eva.pipeline.configuration;

public class BeanNames {

    public static final String GENE_READER = "gene-reader";
    public static final String NON_ANNOTATED_VARIANTS_READER = "non-annotated-variants-reader";
    public static final String VARIANT_ANNOTATION_READER = "variant-annotation-reader";
    public static final String VARIANT_READER = "variant-reader";

    public static final String GENE_WRITER = "gene-writer";
    public static final String VEP_INPUT_WRITER = "vep-input-writer";
    public static final String VARIANT_ANNOTATION_WRITER = "variant-annotation-writer";
    public static final String VARIANT_WRITER = "variant-writer";

    public static final String VEP_ANNOTATION_FLOW = "vep-annotation-flow";
    public static final String VEP_ANNOTATION_OPTIONAL_FLOW = "vep-annotation-optional.flow";
    public static final String PARALLEL_STATISTICS_AND_ANNOTATION = "parallel-statistics-and-annotation-flow";
    public static final String CALCULATE_STATISTICS_OPTIONAL_FLOW = "calculate-statistics-optional-flow";

    public static final String LOAD_VEP_ANNOTATION_STEP = "load-vep-annotation-step";
    public static final String CALCULATE_STATISTICS_STEP = "calculate-statistics-step";
    public static final String CREATE_DATABASE_INDEXES_STEP = "create-database-indexes-step";
    public static final String GENES_LOAD_STEP = "genes-load-step";
    public static final String GENERATE_VEP_ANNOTATION_STEP = "generate-vep-annotation";
    public static final String LOAD_STATISTICS_STEP = "load-statistics-step";
    public static final String LOAD_VARIANTS_STEP = "load-variants-step";
    public static final String GENERATE_VEP_INPUT_STEP = "generate-vep-input-step";

    public static final String AGGREGATED_VCF_JOB = "aggregated-vcf-job";
    public static final String ANNOTATE_VARIANTS_JOB = "annotate-variants-job";
    public static final String INIT_DATABASE_JOB = "init-database-job";
    public static final String GENOTYPED_VCF_JOB = "genotyped-vcf-job";
    public static final String CALCULATE_STATISTICS_JOB = "calculate-statistics-job";

}
