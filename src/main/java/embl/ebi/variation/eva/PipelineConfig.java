package embl.ebi.variation.eva;

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by jmmut on 2015-09-30.
 *
 * Class to extract configuration from properties files and from command line.
 * Default values are in resources/application.properties
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
//@Component
//@ConfigurationProperties
public class PipelineConfig {
    // common
    @Value("${input}")              public String input;
    @Value("${outputDir}")          public String outputDir;
    @Value("${pedigree}")           public String pedigree;
    @Value("${dbName}")             public String dbName;
    @Value("${storageEngine}")      public String storageEngine;
    @Value("${compressGenotypes}")  public boolean compressGenotypes;
    @Value("${compressExtension}")  public String compressExtension;
    @Value("${includeSrc}")         public String includeSrc;
//    @Value("${credentials}")        public String credentials;
//    @Value("${opencga.configfile}") public String configFile;
    @Value("${opencga.app.home}")   public String appHome;
    @Value("${fileId}")             public String fileId;
    @Value("${aggregated}")         public VariantSource.Aggregation aggregated;
    @Value("${studyType}")          public VariantStudy.StudyType studyType;
    @Value("${studyName}")          public String studyName;
    @Value("${studyId}")            public String studyId;

    // transform

    // load
    @Value("${loadThreads}")        public Integer loadThreads;
    // Integer bulkSize, batchSize?

    //stats
    @Value("${calculateStats}")     public boolean calculateStats;
    @Value("${overwriteStats}")     public boolean overwriteStats;

    // annotation
    @Value("${annotate}")           public boolean annotate;

    // job repository DB
    @Value("${jobRepositoryDriverClassName}")   public String jobRepositoryDriverClassName;
    @Value("${jobRepositoryUrl}")               public String jobRepositoryUrl;
    @Value("${jobRepositoryUsername}")          public String jobRepositoryUsername;
    @Value("${jobRepositoryPassword}")          public String jobRepositoryPassword;
}
