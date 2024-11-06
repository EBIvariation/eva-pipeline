package uk.ac.ebi.eva.pipeline.configuration.jobs.steps;

import com.mongodb.client.model.Filters;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.configuration.BeanNames;
import uk.ac.ebi.eva.pipeline.configuration.jobs.GenotypedVcfJobConfiguration;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.configuration.TemporaryRuleConfiguration;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;


@RunWith(SpringRunner.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {GenotypedVcfJobConfiguration.class, BatchTestConfiguration.class, TemporaryRuleConfiguration.class})
public class GenotypedVcfTestSkipStructuralVariant {
    private static final String SMALL_STRUCTURAL_VARIANTS_VCF_FILE = "/input-files/vcf/small_structural_variant.vcf.gz";

    private static final String SMALL_STRUCTURAL_VARIANTS_VCF_FILE_REF_ALT_STARTS_WITH_SAME_ALLELE = "/input-files/vcf/small_invalid_variant.vcf.gz";

    private static final String COLLECTION_VARIANTS_NAME = "variants";

    private static final String databaseName = "test_invalid_variant_db";

    @Autowired
    @Rule
    public TemporaryMongoRule mongoRule;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Before
    public void setUp() throws Exception {
        mongoRule.getTemporaryDatabase(databaseName).drop();
    }

    @Test
    public void loaderStepShouldSkipStructuralVariants() throws Exception {
        // When the execute method in variantsLoad is executed
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(databaseName)
                .inputStudyId("1")
                .inputVcf(getResource(SMALL_STRUCTURAL_VARIANTS_VCF_FILE).getAbsolutePath())
                .inputVcfAggregation("NONE")
                .inputVcfId("1")
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_VARIANTS_STEP, jobParameters);

        //Then variantsLoad step should complete correctly
        assertCompleted(jobExecution);

        // And the number of documents in the DB should be 1 as all other variants are invalid
        assertEquals(1, mongoRule.getCollection(databaseName, COLLECTION_VARIANTS_NAME).count());
        assertEquals(1, mongoRule.getCollection(databaseName, COLLECTION_VARIANTS_NAME).countDocuments(Filters.eq("_id", "1_152739_A_G")));
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
    @Ignore
    @Test
    public void loaderStepShouldSkipStructuralVariantsWhereRefAndAltStartsWithSameAllele() throws Exception {
        // When the execute method in variantsLoad is executed
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(databaseName)
                .inputStudyId("1")
                .inputVcf(getResource(SMALL_STRUCTURAL_VARIANTS_VCF_FILE_REF_ALT_STARTS_WITH_SAME_ALLELE).getAbsolutePath())
                .inputVcfAggregation("NONE")
                .inputVcfId("1")
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_VARIANTS_STEP, jobParameters);

        //Then variantsLoad step should complete correctly
        assertCompleted(jobExecution);

        assertEquals(0, mongoRule.getCollection(databaseName, COLLECTION_VARIANTS_NAME).count());
    }
}