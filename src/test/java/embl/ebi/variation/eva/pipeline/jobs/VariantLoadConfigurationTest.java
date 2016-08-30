/*
 * Copyright 2015-2016 EMBL - European Bioinformatics Institute
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
package embl.ebi.variation.eva.pipeline.jobs;

import embl.ebi.variation.eva.VariantJobsArgs;
import embl.ebi.variation.eva.pipeline.config.CommonConfig;
import embl.ebi.variation.eva.pipeline.steps.VariantsLoad;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.FileInputStream;
import java.net.UnknownHostException;
import java.util.zip.GZIPInputStream;

import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.countRows;
import static embl.ebi.variation.eva.pipeline.jobs.JobTestUtils.getLines;
import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.core.variant.VariantStorageManager.VARIANT_SOURCE;

/**
 * Created by jmmut on 2015-10-14.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {VariantLoadConfiguration.class, CommonConfig.class, JobLauncherTestUtils.class})
public class VariantLoadConfigurationTest {

    private static final String FILE_20 = "/small20.vcf.gz";
    private static final String VALID_LOAD = "VariantLoadConfigurationTest_v";
    private static final String INVALID_LOAD = "VariantLoadConfigurationTest_i";

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private VariantJobsArgs variantJobsArgs;

    private ObjectMap variantOptions;
    private ObjectMap pipelineOptions;
    @Test
    public void loadStepShouldLoadAllVariants() throws Exception {
        //Given a valid VCF input file
        String input = FILE_20;
        String dbName = VALID_LOAD;

        pipelineOptions.put("input.vcf", input);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        pipelineOptions.put(VariantsLoad.SKIP_LOAD, false);

        String outputDir = pipelineOptions.getString("output.dir");

        variantOptions.put(VARIANT_SOURCE, new VariantSource(
                input,
                "1",
                "1",
                "studyName",
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE));

        //and a variants transform step already executed
        File transformedVcfVariantsFile =
                new File(VariantLoadConfigurationTest.class.getResource("/small20.vcf.gz.variants.json.gz").getFile());
        File tmpTransformedVcfVariantsFile = new File(outputDir, transformedVcfVariantsFile.getName());
        FileUtils.copyFile(transformedVcfVariantsFile, tmpTransformedVcfVariantsFile);

        File transformedVariantsFile =
                new File(VariantLoadConfigurationTest.class.getResource("/small20.vcf.gz.file.json.gz").getFile());
        File tmpTransformedVariantsFile = new File(outputDir, transformedVariantsFile.getName());
        FileUtils.copyFile(transformedVariantsFile, tmpTransformedVariantsFile);

        // When the execute method in variantsLoad is executed
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("Load variants");

        //Then variantsLoad step should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // And the number of documents in db should be the same number of line of the vcf transformed file
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());
        long lines = getLines(new GZIPInputStream(new FileInputStream(transformedVcfVariantsFile)));

        assertEquals(countRows(iterator), lines);

        tmpTransformedVcfVariantsFile.delete();
        tmpTransformedVariantsFile.delete();
    }

    @Test
    public void loadStepShouldFailBecauseOpenCGAHomeIsWrong() throws JobExecutionException {
        String input = VariantLoadConfigurationTest.class.getResource(FILE_20).getFile();
        String outdir = input;
        String dbName = INVALID_LOAD;

        Config.setOpenCGAHome("");

        pipelineOptions.put("input.vcf", input);
        pipelineOptions.put("output.dir", outdir);
        variantOptions.put(VariantStorageManager.DB_NAME, dbName);
        pipelineOptions.put(VariantsLoad.SKIP_LOAD, false);

        VariantSource source = (VariantSource) variantOptions.get(VariantStorageManager.VARIANT_SOURCE);

        variantOptions.put(VariantStorageManager.VARIANT_SOURCE, new VariantSource(
                input,
                source.getFileId(),
                source.getStudyId(),
                source.getStudyName(),
                source.getType(),
                source.getAggregation()));

        JobExecution jobExecution = jobLauncherTestUtils.launchStep("Load variants");

        assertEquals(input, pipelineOptions.getString("input.vcf"));
        assertEquals(ExitStatus.FAILED.getExitCode(), jobExecution.getExitStatus().getExitCode());
    }

    @BeforeClass
    public static void beforeTests() throws UnknownHostException {
        cleanDBs();
    }

    @Before
    public void setUp() throws Exception {
        //re-initialize common config before each test
        variantJobsArgs.loadArgs();
        pipelineOptions = variantJobsArgs.getPipelineOptions();
        variantOptions = variantJobsArgs.getVariantOptions();
    }

    @AfterClass
    public static void afterTests() throws UnknownHostException {
        cleanDBs();
    }

    private static void cleanDBs() throws UnknownHostException {
        JobTestUtils.cleanDBs(VALID_LOAD, INVALID_LOAD);
    }

}
