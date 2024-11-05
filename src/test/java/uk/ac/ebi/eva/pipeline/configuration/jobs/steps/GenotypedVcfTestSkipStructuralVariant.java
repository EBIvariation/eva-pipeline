package uk.ac.ebi.eva.pipeline.configuration.jobs.steps;

import com.mongodb.client.model.Filters;
import org.junit.Before;
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

    private static final int EXPECTED_VARIANTS = 1;

    private static final String SMALL_VCF_FILE = "/input-files/vcf/small_structural_variant.vcf.gz";

    private static final String COLLECTION_VARIANTS_NAME = "variants";

    private static final String databaseName = "test_invalid_variant_db";

    @Autowired
    @Rule
    public TemporaryMongoRule mongoRule;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    private String input;

    @Before
    public void setUp() throws Exception {
        mongoRule.getTemporaryDatabase(databaseName).drop();
        input = getResource(SMALL_VCF_FILE).getAbsolutePath();
    }

    @Test
    public void loaderStepShouldLoadAllVariants() throws Exception {
        // When the execute method in variantsLoad is executed
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(databaseName)
                .inputStudyId("1")
                .inputVcf(input)
                .inputVcfAggregation("NONE")
                .inputVcfId("1")
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_VARIANTS_STEP, jobParameters);

        //Then variantsLoad step should complete correctly
        assertCompleted(jobExecution);

        // And the number of documents in the DB should be 1 as all other variants are invalid
        assertEquals(EXPECTED_VARIANTS, mongoRule.getCollection(databaseName, COLLECTION_VARIANTS_NAME).count());

        assertEquals(1, mongoRule.getCollection(databaseName, COLLECTION_VARIANTS_NAME).countDocuments(Filters.eq("_id", "1_152739_A_G")));
    }
}