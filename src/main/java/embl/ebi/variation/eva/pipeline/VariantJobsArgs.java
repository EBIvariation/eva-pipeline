package embl.ebi.variation.eva.pipeline;

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Created by diego on 19/05/2016.
 *
 * // TODO: 20/05/2016 add type validator?
 */
@Component
public class VariantJobsArgs {
    private static final Logger logger = LoggerFactory.getLogger(VariantJobsArgs.class);

    ////common
    @Value("${input}") private String input;
    @Value("${compressExtension}") private String compressExtension;

    ////opencga
    @Value("${fileId}") private String fileId;
    @Value("${studyType}") private String studyType;
    @Value("${studyName}") private String studyName;
    @Value("${studyId}") private String studyId;
    @Value("${dbName}") private String dbName;
    @Value("${compressGenotypes}") private String compressGenotypes;
    @Value("${overwriteStats:false}") private boolean overwriteStats;
    @Value("${calculateStats:false}") private boolean calculateStats;
    @Value("${includeSamples:false}") private String includeSamples;
    @Value("${annotate:false}") private boolean annotate;
    @Value("${includeSrc}") private String includeSrc;
    @Value("${includeStats:false}")private String includeStats;
    @Value("${aggregated}") private String aggregated;


    @Value("${opencga.app.home}") private String opencgaAppHome;

    ////pipeline
    @Value("${outputDir}") private String outputDir;
    @Value("${pedigree}") private String pedigree;

    //steps
    @Value("${skipLoad:false}") private boolean skipLoad;
    @Value("${skipStatsCreate:false}") private boolean skipStatsCreate;
    @Value("${skipStatsLoad:false}") private boolean skipStatsLoad;
    @Value("${skipAnnotGenerateInput:false}") private boolean skipAnnotGenerateInput;
    @Value("${skipAnnotCreate:false}") private boolean skipAnnotCreate;
    @Value("${skipAnnotLoad:false}") private boolean skipAnnotLoad;

    //VEP
    @Value("${vepInput}") private String vepInput;
    @Value("${vepOutput}") private String vepOutput;
    @Value("${vepPath}") private String vepPath;
    @Value("${vepCacheDirectory}") private String vepCacheDirectory;
    @Value("${vepCacheVersion}") private String vepCacheVersion;
    @Value("${vepSpecies}") private String vepSpecies;
    @Value("${vepFasta}") private String vepFasta;
    @Value("${vepNumForks}") private String vepNumForks;

    private ObjectMap variantOptions  = new ObjectMap();
    private ObjectMap pipelineOptions  = new ObjectMap();

    @PostConstruct
    public void loadArgs() {
        logger.info("Load args");

        // TODO validation checks for all the parameters
        Config.setOpenCGAHome(opencgaAppHome);

        loadVariantOptions();
        loadPipelineOptions();
    }

    private void loadVariantOptions(){
        VariantSource source = new VariantSource(
                input,
                fileId,
                studyId,
                studyName,
                VariantStudy.StudyType.valueOf(studyType),
                VariantSource.Aggregation.valueOf(aggregated));

        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, source);
        variantOptions.put(VariantStorageManager.OVERWRITE_STATS, overwriteStats);
        variantOptions.put(VariantStorageManager.INCLUDE_SRC, VariantStorageManager.IncludeSrc.parse(includeSrc));
        variantOptions.put("compressExtension", compressExtension);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        variantOptions.put(VariantStorageManager.COMPRESS_GENOTYPES, Boolean.parseBoolean(compressGenotypes));
        variantOptions.put(VariantStorageManager.INCLUDE_STATS, Boolean.parseBoolean(includeStats));

        logger.debug("Using as input: {}", input);

        variantOptions.put(VariantStorageManager.CALCULATE_STATS, false);   // this is tested by hand
        variantOptions.put(VariantStorageManager.INCLUDE_SAMPLES, true);   // TODO rename samples to genotypes
        variantOptions.put(VariantStorageManager.ANNOTATE, false);

        logger.debug("Using as variantOptions: {}", variantOptions.entrySet().toString());
    }

    private void loadPipelineOptions(){
        pipelineOptions.put("opencgaAppHome", opencgaAppHome);
        pipelineOptions.put("input", input);
        pipelineOptions.put("compressExtension", compressExtension);
        pipelineOptions.put("outputDir", outputDir);
        pipelineOptions.put("pedigree", pedigree);
        pipelineOptions.put("skipLoad", skipLoad);
        pipelineOptions.put("skipStatsCreate", skipStatsCreate);
        pipelineOptions.put("skipStatsLoad", skipStatsLoad);
        pipelineOptions.put("skipAnnotGenerateInput", skipAnnotGenerateInput);
        pipelineOptions.put("skipAnnotCreate", skipAnnotCreate);
        pipelineOptions.put("skipAnnotLoad", skipAnnotLoad);
        pipelineOptions.put("vepInput", vepInput);
        pipelineOptions.put("vepOutput", vepOutput);
        pipelineOptions.put("vepPath", vepPath);
        pipelineOptions.put("vepCacheDirectory", vepCacheDirectory);
        pipelineOptions.put("vepCacheVersion", vepCacheVersion);
        pipelineOptions.put("vepSpecies", vepSpecies);
        pipelineOptions.put("vepFasta", vepFasta);
        pipelineOptions.put("vepNumForks", vepNumForks);

        logger.debug("Using as pipelineOptions: {}", pipelineOptions.entrySet().toString());
    }

    public void setVariantOptions(ObjectMap variantOptions) {
        this.variantOptions = variantOptions;
    }

    public void setPipelineOptions(ObjectMap pipelineOptions) {
        this.pipelineOptions = pipelineOptions;
    }

    public ObjectMap getVariantOptions() {
        return variantOptions;
    }

    public ObjectMap getPipelineOptions() {
        return pipelineOptions;
    }
}
