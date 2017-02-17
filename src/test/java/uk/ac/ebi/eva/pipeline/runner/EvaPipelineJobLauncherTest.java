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
package uk.ac.ebi.eva.pipeline.runner;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.rule.OutputCapture;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.pipeline.EvaPipelineJobLauncher;
import uk.ac.ebi.eva.test.rules.PipelineTemporaryFolderRule;
import uk.ac.ebi.eva.test.rules.TemporaryMongoRule;
import uk.ac.ebi.eva.test.utils.GenotypedVcfJobTestUtils;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.GENOTYPED_VCF_JOB;
import static uk.ac.ebi.eva.test.utils.TestFileUtils.getResource;

@RunWith(SpringRunner.class)
@SpringBootTest()
@ActiveProfiles({"test,mongo"})
@TestPropertySource(value = {"classpath:test-mongo.properties"}, properties = "debug=true")
public class EvaPipelineJobLauncherTest {

    private static final String GENOTYPED_PROPERTIES_FILE = "/genotype-test.properties";

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private EvaPipelineJobLauncher evaPipelineJobLauncher;

    @Rule
    public OutputCapture capture = new OutputCapture();

    @Rule
    public TemporaryMongoRule mongoRule = new TemporaryMongoRule();

    @Rule
    public PipelineTemporaryFolderRule temporaryFolderRule = new PipelineTemporaryFolderRule();

    @Test
    public void noJobNameHasBeenProvidedStopsExecution() throws JobExecutionException {
        evaPipelineJobLauncher.setJobNames(null);
        evaPipelineJobLauncher.run();
        assertThat(capture.toString(), containsString("No job name has been provided"));
    }

    @Test
    public void jobProvidedButNoParameters() throws JobExecutionException {
        evaPipelineJobLauncher.setJobNames("genotyped-vcf-job");
        evaPipelineJobLauncher.run();
        assertThat(capture.toString(), containsString("No parameters have been provided"));
    }

    @Test
    public void genotypedVcfJobTest() throws JobExecutionException, IOException, URISyntaxException,
            ClassNotFoundException, StorageManagerException, InstantiationException, IllegalAccessException {
        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        File inputFile = GenotypedVcfJobTestUtils.getInputFile();
        String outputDirStats = temporaryFolderRule.newFolder().getAbsolutePath();
        String outputDirAnnotation = temporaryFolderRule.newFolder().getAbsolutePath();

        File variantsStatsFile = GenotypedVcfJobTestUtils.getVariantsStatsFile(outputDirStats);
        File sourceStatsFile = GenotypedVcfJobTestUtils.getSourceStatsFile(outputDirStats);

        File vepInputFile = GenotypedVcfJobTestUtils.getVepInputFile(outputDirAnnotation);
        File vepOutputFile = GenotypedVcfJobTestUtils.getVepOutputFile(outputDirAnnotation);

        evaPipelineJobLauncher.setJobNames(GENOTYPED_VCF_JOB);
        evaPipelineJobLauncher.run(
                "input.vcf=" + inputFile.getAbsolutePath(),
                "input.vcf.id=" + GenotypedVcfJobTestUtils.INPUT_VCF_ID,
                "input.vcf.aggregation=NONE",
                "input.study.name=small vcf",
                "input.study.id=" + GenotypedVcfJobTestUtils.INPUT_STUDY_ID,
                "input.study.type=COLLECTION",
                "output.dir.annotation=" + outputDirAnnotation,
                "output.dir.statistics=" + outputDirStats,
                "spring.data.mongodb.database=" + databaseName,
                "app.vep.path=" + GenotypedVcfJobTestUtils.getMockVep().getPath(),
                "app.vep.num-forks=",
                "app.vep.cache.path=",
                "app.vep.cache.version=",
                "app.vep.cache.species=",
                "input.fasta=",
                "config.db.read-preference=secondary",
                "db.collections.variants.name=variants",
                "db.collections.files.name=files",
                "db.collections.features.name=features",
                "db.collections.stats.name=populationStatistics"
        );

        assertFalse(jobExplorer.getJobInstances(GENOTYPED_VCF_JOB, 0, 1).isEmpty());
        JobExecution jobExecution = jobExplorer.getJobExecution(jobExplorer.getJobInstances(GENOTYPED_VCF_JOB, 0, 1)
                .get(0).getInstanceId());

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        GenotypedVcfJobTestUtils.checkLoadStep(databaseName);

        GenotypedVcfJobTestUtils.checkCreateStatsStep(variantsStatsFile, sourceStatsFile);

        GenotypedVcfJobTestUtils.checkLoadStatsStep(databaseName);

        GenotypedVcfJobTestUtils.checkAnnotationInput(vepInputFile);

        GenotypedVcfJobTestUtils.checkAnnotationCreateStep(vepInputFile, vepOutputFile);

        GenotypedVcfJobTestUtils.checkOutputFileLength(vepOutputFile);

        GenotypedVcfJobTestUtils.checkLoadedAnnotation(databaseName);

        GenotypedVcfJobTestUtils.checkSkippedOneMalformedLine(jobExecution);
    }

    @Test
    public void genotypedVcfJobTestWithParametersFileAndCommandLineParameters() throws JobExecutionException,
            IOException, URISyntaxException, ClassNotFoundException, StorageManagerException, InstantiationException,
            IllegalAccessException {

        String databaseName = mongoRule.getRandomTemporaryDatabaseName();
        File inputFile = GenotypedVcfJobTestUtils.getInputFile();
        String outputDirStats = temporaryFolderRule.newFolder().getAbsolutePath();
        String outputDirAnnotation = temporaryFolderRule.newFolder().getAbsolutePath();

        File variantsStatsFile = GenotypedVcfJobTestUtils.getVariantsStatsFile(outputDirStats);
        File sourceStatsFile = GenotypedVcfJobTestUtils.getSourceStatsFile(outputDirStats);

        File vepInputFile = GenotypedVcfJobTestUtils.getVepInputFile(outputDirAnnotation);
        File vepOutputFile = GenotypedVcfJobTestUtils.getVepOutputFile(outputDirAnnotation);

        //Set properties file to read
        evaPipelineJobLauncher.setPropertyFilePath(getResource(GENOTYPED_PROPERTIES_FILE).getAbsolutePath());

        evaPipelineJobLauncher.setJobNames(GENOTYPED_VCF_JOB);
        evaPipelineJobLauncher.run(
                "input.vcf=" + inputFile.getAbsolutePath(),
                "input.vcf.id=" + GenotypedVcfJobTestUtils.INPUT_VCF_ID,
                "input.study.id=" + GenotypedVcfJobTestUtils.INPUT_STUDY_ID,
                "output.dir.annotation=" + outputDirAnnotation,
                "output.dir.statistics=" + outputDirStats,
                "spring.data.mongodb.database=" + databaseName,
                "app.vep.path=" + GenotypedVcfJobTestUtils.getMockVep().getPath()
        );

        assertFalse(jobExplorer.getJobInstances(GENOTYPED_VCF_JOB, 0, 1).isEmpty());
        JobExecution jobExecution = jobExplorer.getJobExecution(jobExplorer.getJobInstances(GENOTYPED_VCF_JOB, 0, 1)
                .get(0).getInstanceId());

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        GenotypedVcfJobTestUtils.checkLoadStep(databaseName);

        GenotypedVcfJobTestUtils.checkCreateStatsStep(variantsStatsFile, sourceStatsFile);

        GenotypedVcfJobTestUtils.checkLoadStatsStep(databaseName);

        GenotypedVcfJobTestUtils.checkAnnotationInput(vepInputFile);

        GenotypedVcfJobTestUtils.checkAnnotationCreateStep(vepInputFile, vepOutputFile);

        GenotypedVcfJobTestUtils.checkOutputFileLength(vepOutputFile);

        GenotypedVcfJobTestUtils.checkLoadedAnnotation(databaseName);

        GenotypedVcfJobTestUtils.checkSkippedOneMalformedLine(jobExecution);
    }
}
