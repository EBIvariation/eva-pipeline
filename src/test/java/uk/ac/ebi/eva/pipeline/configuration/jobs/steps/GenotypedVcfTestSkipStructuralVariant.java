package uk.ac.ebi.eva.pipeline.configuration.jobs.steps;

import com.mongodb.client.model.Filters;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.jobs.GenotypedVcfJobConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils;
import uk.ac.ebi.eva.test.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.test.utils.PipelineTemporaryFolderUtil;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.ac.ebi.eva.test.configuration.BatchTestConfiguration.JOB_GENOTYPE_VCF_JOB;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;


@ExtendWith(SpringExtension.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@ContextConfiguration(classes = {GenotypedVcfJobConfiguration.class, BatchTestConfiguration.class})
public class GenotypedVcfTestSkipStructuralVariant extends MongoTestContainerHelper {
    private static final String SMALL_STRUCTURAL_VARIANTS_VCF_FILE = "/input-files/vcf/small_structural_variant.vcf.gz";

    private static final String SMALL_STRUCTURAL_VARIANTS_VCF_FILE_REF_ALT_STARTS_WITH_SAME_ALLELE = "/input-files/vcf/small_invalid_variant.vcf.gz";

    private static final String COLLECTION_VARIANTS_NAME = "variants";

    private static final String DB_NAME = "test_invalid_variant_db";

    public PipelineTemporaryFolderUtil temporaryFolderUtil = new PipelineTemporaryFolderUtil();

    @Autowired
    @Qualifier(JOB_GENOTYPE_VCF_JOB)
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private MongoMappingContext mongoMappingContext;

    @Autowired
    private BatchTestConfiguration batchTestConfiguration;

    private MongoTemplate mongoTemplate;

    @BeforeEach
    public void setUp() throws Exception {
        mongoTemplate = batchTestConfiguration.getMongoTemplate(DB_NAME, mongoMappingContext);
        mongoTemplate.getDb().drop();
    }

    @AfterEach
    void cleanDb() {
        mongoTemplate.getDb().drop();
    }

    @Test
    public void loaderStepShouldSkipStructuralVariants() throws Exception {
        File fasta = temporaryFolderUtil.newFile();
        String assemblyReport = GenotypedVcfJobTestUtils.getAssemblyReport();

        // When the execute method in variantsLoad is executed
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(DB_NAME)
                .inputFasta(fasta.getAbsolutePath())
                .inputAssemblyReport(assemblyReport)
                .inputStudyId("1")
                .inputVcf(getResource(SMALL_STRUCTURAL_VARIANTS_VCF_FILE).getAbsolutePath())
                .inputVcfAggregation("NONE")
                .inputVcfId("1")
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_VARIANTS_STEP, jobParameters);

        //Then variantsLoad step should complete correctly
        assertCompleted(jobExecution);

        // And the number of documents in the DB should be 1 as all other variants are invalid
        assertEquals(1, mongoTemplate.getDb().getCollection(COLLECTION_VARIANTS_NAME).countDocuments());
        assertEquals(1, mongoTemplate.getDb().getCollection(COLLECTION_VARIANTS_NAME).countDocuments(Filters.eq("_id", "CM000093.4_152739_A_G")));
    }

    /*
     * This test case represents a special case of structural variants that should be skipped, but due to a bug makes its
     * way into the DB.
     *
     * The variant has ref as "G" and alt as "G[2:421681[", which means it fits the definition of a structural variant
     * and should be skipped by the variant processor that filters out structural variants.
     *
     * But instead what is currently happening is that after the variant is read, the normalization process in variant
     * reader removes the prefix G and the variant is eventually reduced to ref "" and alt "[2:421681[", which does not get
     * parsed correctly by the regex in @ExcludeStructuralVariantsProcessor and makes its way into the DB.
     *
     * Currently, the test case fails therefore we are skipping it for now. It should start passing once we have fixed the
     * problem with the normalization in the variant reader.
     */
    @Disabled
    @Test
    public void loaderStepShouldSkipStructuralVariantsWhereRefAndAltStartsWithSameAllele() throws Exception {
        File fasta = temporaryFolderUtil.newFile();
        String assemblyReport = GenotypedVcfJobTestUtils.getAssemblyReport();

        // When the execute method in variantsLoad is executed
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(DB_NAME)
                .inputFasta(fasta.getAbsolutePath())
                .inputAssemblyReport(assemblyReport)
                .inputStudyId("1")
                .inputVcf(getResource(SMALL_STRUCTURAL_VARIANTS_VCF_FILE_REF_ALT_STARTS_WITH_SAME_ALLELE).getAbsolutePath())
                .inputVcfAggregation("NONE")
                .inputVcfId("1")
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_VARIANTS_STEP, jobParameters);

        //Then variantsLoad step should complete correctly
        assertCompleted(jobExecution);

        assertEquals(0, mongoTemplate.getDb().getCollection(COLLECTION_VARIANTS_NAME).countDocuments());
    }
}