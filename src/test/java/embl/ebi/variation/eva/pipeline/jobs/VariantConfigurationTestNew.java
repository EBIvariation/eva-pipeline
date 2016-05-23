package embl.ebi.variation.eva.pipeline.jobs;

import embl.ebi.variation.eva.pipeline.listeners.VariantJobParametersListener;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.lib.common.Config;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static embl.ebi.variation.eva.pipeline.jobs.VariantConfigurationTest.FILE_20;

/**
 * Created by diego on 23/05/2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { VariantConfiguration.class, VariantJobParametersListener.class})
public class VariantConfigurationTestNew {

    static String input = VariantConfigurationTest.class.getResource(FILE_20).getFile();


 /*   @Qualifier("getPipelineOptions")
    @Autowired
    private ObjectMap pipelineOptions;

    @Qualifier("getVariantOptions")
    @Autowired
    private ObjectMap variantOptions;*/

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @BeforeClass
    public static void beforeClass() {
        System.setProperty("input", input);
        System.setProperty("overwriteStats", "false");
    }

    @Test
    public void ss() throws Exception {

        //Config.setOpenCGAHome(pipelineOptions.getString("opencga.app.home"));

        // test a complete job
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

    }

}