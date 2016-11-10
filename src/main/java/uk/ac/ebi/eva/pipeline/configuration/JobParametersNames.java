package uk.ac.ebi.eva.pipeline.configuration;

public class JobParametersNames {

    /*
     * Input
     */
    
    public static final String INPUT_VCF = "input.vcf";
    
    public static final String INPUT_VCF_ID = "input.vcf.id";
    
    public static final String INPUT_VCF_AGGREGATION = "input.vcf.aggregation";
    
    public static final String INPUT_STUDY_NAME = "input.study.name";
    
    public static final String INPUT_STUDY_ID = "input.study.id";
    
    public static final String INPUT_STUDY_TYPE = "input.study.type";
    
    public static final String INPUT_PEDIGREE = "input.pedigree";
    
    public static final String INPUT_GTF = "input.gtf";

    public static final String INPUT_FASTA = "input.fasta";

    /*
     * Output
     */
    
    public static final String OUTPUT_DIR = "output.dir";
    
    public static final String OUTPUT_DIR_ANNOTATION = "output.dir.annotation";
    
    public static final String OUTPUT_DIR_STATISTICS = "output.dir.statistics";


    /*
     * Database infrastructure (most parameters read from OpenCGA "conf" folder)
     */
    
    public static final String CONFIG_DB_HOSTS = "config.db.hosts";
    
    public static final String CONFIG_DB_AUTHENTICATIONDB = "config.db.authentication-db";
    
    public static final String CONFIG_DB_USER = "config.db.user";
    
    public static final String CONFIG_DB_PASSWORD = "config.db.password";
    
    public static final String CONFIG_DB_READPREFERENCE = "config.db.read-preference";
    
    
    /*
     * Database and collections
     */
    
    public static final String DB_NAME = "db.name";
    
    public static final String DB_COLLECTIONS_VARIANTS_NAME = "db.collections.variants.name";
    
    public static final String DB_COLLECTIONS_FILES_NAME = "db.collections.files.name";
    
    public static final String DB_COLLECTIONS_FEATURES_NAME = "db.collections.features.name";
    
    public static final String DB_COLLECTIONS_STATS_NAME = "db.collections.stats.name";
    
    
    /*
     * Skip and overwrite steps
     */
    
    public static final String ANNOTATION_SKIP = "annotation.skip";
    
    public static final String STATISTICS_SKIP = "statistics.skip";

    public static final String STATISTICS_OVERWRITE = "statistics.overwrite";
    
    
    /*
     * OpenCGA
     */
    
    public static final String APP_OPENCGA_PATH = "app.opencga.path";

    
    /*
     * Variant Effect Predictor (VEP)
     */
    
    public static final String APP_VEP_PATH = "app.vep.path";
    
    public static final String APP_VEP_CACHE_PATH = "app.vep.cache.path";
    
    public static final String APP_VEP_CACHE_VERSION = "app.vep.cache.version";
    
    public static final String APP_VEP_CACHE_SPECIES = "app.vep.cache.species";
    
    public static final String APP_VEP_NUMFORKS = "app.vep.num-forks";

    
    /*
     * Other configuration
     */
    
    public static final String CONFIG_RESTARTABILITY_ALLOW = "config.restartability.allow";
}
