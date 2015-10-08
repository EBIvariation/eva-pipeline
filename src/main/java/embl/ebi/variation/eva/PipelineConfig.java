package uk.ac.ebi.variation;

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

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
    @Value("${input}")              public String input;
    @Value("${outputDir}")          public String outputDir;
    @Value("${pedigree}")           public String pedigree;
    @Value("${dbName}")             public String dbName;
    @Value("${samples}")            public String samples;
    @Value("${storageEngine}")      public String storageEngine;
    @Value("${compressGenotypes}")  public String compressGenotypes;
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

    // job repository DB
    @Value("${jobRepositoryDriverClassName}")   public String jobRepositoryDriverClassName;
    @Value("${jobRepositoryUrl}")               public String jobRepositoryUrl;
    @Value("${jobRepositoryUsername}")          public String jobRepositoryUsername;
    @Value("${jobRepositoryPassword}")          public String jobRepositoryPassword;
}
