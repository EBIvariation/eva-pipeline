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
package uk.ac.ebi.eva.pipeline.configuration.jobs.steps;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
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
import uk.ac.ebi.eva.pipeline.io.contig.ContigNaming;
import uk.ac.ebi.eva.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.test.configuration.TemporaryRuleConfiguration;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.utils.EvaJobParameterBuilder;

import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertCompleted;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.assertFailed;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * Test for {@link LoadVariantsStepConfiguration}
 */
@RunWith(SpringRunner.class)
@ActiveProfiles({Application.VARIANT_WRITER_MONGO_PROFILE, Application.VARIANT_ANNOTATION_MONGO_PROFILE})
@TestPropertySource({"classpath:common-configuration.properties", "classpath:test-mongo.properties"})
@ContextConfiguration(classes = {GenotypedVcfJobConfiguration.class, BatchTestConfiguration.class, TemporaryRuleConfiguration.class})
public class LoadVariantsStepTest {

    private static final int EXPECTED_VARIANTS = 300;
    private static final String SMALL_VCF_FILE = "/input-files/vcf/genotyped.vcf.gz";
    private static final String ASSEMBLY_REPORT = "/input-files/assembly-report/assembly_report.txt";
    private static final String ASSEMBLY_REPORT_MISSING_CONTIG = "/input-files/assembly-report/assembly_report_missing_contig.txt";
    private static final String ASSEMBLY_REPORT_CANT_TRANSLATE_CONTIG = "/input-files/assembly-report/assembly_report_cant_translate_contig.txt";

    private static final String COLLECTION_VARIANTS_NAME = "variants";

    @Autowired
    @Rule
    public TemporaryMongoRule mongoRule;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    private String input;
    private String assemblyReport;
    private String assemblyReportMissingContig;
    private String assemblyReportCantTranslateContig;

    @Before
    public void setUp() throws Exception {
        input = getResource(SMALL_VCF_FILE).getAbsolutePath();
        assemblyReport = "file://" + getResource(ASSEMBLY_REPORT).getAbsolutePath();
        assemblyReportMissingContig = "file://" + getResource(ASSEMBLY_REPORT_MISSING_CONTIG).getAbsolutePath();
        assemblyReportCantTranslateContig = "file://" + getResource(ASSEMBLY_REPORT_CANT_TRANSLATE_CONTIG).getAbsolutePath();
    }

    @Test
    public void loaderStepShouldLoadAllVariants() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();

        // When the execute method in variantsLoad is executed
        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(databaseName)
                .inputStudyId("1")
                .inputVcf(input)
                .inputVcfAggregation("NONE")
                .inputVcfId("1")
                .inputAssemblyReport(assemblyReport)
                .contigNaming(ContigNaming.NO_REPLACEMENT)
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_VARIANTS_STEP, jobParameters);

        //Then variantsLoad step should complete correctly
        assertCompleted(jobExecution);

        // And the number of documents in the DB should be equals to the number of lines in the VCF file
        assertEquals(EXPECTED_VARIANTS, mongoRule.getCollection(databaseName, COLLECTION_VARIANTS_NAME).countDocuments());

        // Contigs should not be translated
        MongoCursor<Document> mongoCursor = mongoRule.getCollection(databaseName, COLLECTION_VARIANTS_NAME).find().iterator();
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(mongoCursor, 0), false)
                .map(doc -> doc.get("chr"))
                .allMatch(chr -> chr.equals("20"));
    }

    @Test
    public void loaderStepShouldLoadAllVariantsWithTranslatedContig() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(databaseName)
                .inputStudyId("1")
                .inputVcf(input)
                .inputVcfAggregation("NONE")
                .inputVcfId("1")
                .inputAssemblyReport(assemblyReport)
                .contigNaming(ContigNaming.REFSEQ)
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_VARIANTS_STEP, jobParameters);

        //Then variantsLoad step should complete correctly
        assertCompleted(jobExecution);

        // And the number of documents in the DB should be equals to the number of lines in the VCF file
        assertEquals(EXPECTED_VARIANTS, mongoRule.getCollection(databaseName, COLLECTION_VARIANTS_NAME).countDocuments());

        // All the contigs should be translated
        MongoCursor<Document> mongoCursor = mongoRule.getCollection(databaseName, COLLECTION_VARIANTS_NAME).find().iterator();
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(mongoCursor, 0), false)
                .map(doc -> doc.get("chr"))
                .allMatch(chr -> chr.equals("CM000095.5"));
    }


    @Test
    public void loaderStepShouldFailWithContigNotFound() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(databaseName)
                .inputStudyId("1")
                .inputVcf(input)
                .inputVcfAggregation("NONE")
                .inputVcfId("1")
                .inputAssemblyReport(assemblyReportMissingContig)
                .contigNaming(ContigNaming.REFSEQ)
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_VARIANTS_STEP, jobParameters);
        assertFailed(jobExecution);
        assertTrue(jobExecution.toString().contains("java.lang.IllegalArgumentException: Contig '20' was not found in the assembly report!"));
    }


    @Test
    public void loaderStepShouldFailWithContigCouldNotBeTranslated() throws Exception {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();

        JobParameters jobParameters = new EvaJobParameterBuilder()
                .collectionVariantsName(COLLECTION_VARIANTS_NAME)
                .databaseName(databaseName)
                .inputStudyId("1")
                .inputVcf(input)
                .inputVcfAggregation("NONE")
                .inputVcfId("1")
                .inputAssemblyReport(assemblyReportCantTranslateContig)
                .contigNaming(ContigNaming.REFSEQ)
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.LOAD_VARIANTS_STEP, jobParameters);
        assertFailed(jobExecution);
        assertTrue(jobExecution.toString().contains("java.lang.IllegalArgumentException: GenBank and RefSeq not identical in the assembly report for contig '20' and RefSeq is not empty"));
    }
}