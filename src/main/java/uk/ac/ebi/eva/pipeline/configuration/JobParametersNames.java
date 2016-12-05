package uk.ac.ebi.eva.pipeline.configuration;

import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageManager;

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
     * Database infrastructure (Spring Data)
     */
    
    public static final String CONFIG_DB_HOSTS = "spring.data.mongodb.host";
    
    public static final String CONFIG_DB_AUTHENTICATIONDB = "spring.data.mongodb.authentication-database";
    
    public static final String CONFIG_DB_USER = "spring.data.mongodb.username";
    
    public static final String CONFIG_DB_PASSWORD = "spring.data.mongodb.password";
    
    public static final String CONFIG_DB_READPREFERENCE = "config.db.read-preference";
    
    
    /*
     * Database and collections
     */
    
    public static final String DB_NAME = "spring.data.mongodb.database";
    
    public static final String DB_COLLECTIONS_VARIANTS_NAME = "db.collections.variants.name";
    
    public static final String DB_COLLECTIONS_FILES_NAME = "db.collections.files.name";
    
    public static final String DB_COLLECTIONS_FEATURES_NAME = "db.collections.features.name";
    
    public static final String DB_COLLECTIONS_STATISTICS_NAME = "db.collections.stats.name";
    
    
    /*
     * Skip and overwrite steps
     */
    
    public static final String ANNOTATION_SKIP = "annotation.skip";
    
    public static final String STATISTICS_SKIP = "statistics.skip";

    public static final String STATISTICS_OVERWRITE = "statistics.overwrite";
    
    
    /*
     * OpenCGA (parameters read from OpenCGA "conf" folder)
     */
    
    public static final String APP_OPENCGA_PATH = "app.opencga.path";

    public static final String OPENCGA_DB_NAME = MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_NAME;
    
    public static final String OPENCGA_DB_HOSTS = MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_HOSTS;
    
    public static final String OPENCGA_DB_AUTHENTICATIONDB = MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_AUTHENTICATION_DB;
    
    public static final String OPENCGA_DB_USER = MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_USER;
    
    public static final String OPENCGA_DB_PASSWORD = MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_PASS;

    public static final String OPENCGA_DB_COLLECTIONS_VARIANTS_NAME = MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_VARIANTS;
    
    public static final String OPENCGA_DB_COLLECTIONS_FILES_NAME = MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_FILES;
    
    
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
