package uk.ac.ebi.eva.pipeline.jobs.steps;

import org.opencb.biodata.formats.pedigree.io.PedigreePedReader;
import org.opencb.biodata.formats.pedigree.io.PedigreeReader;
import org.opencb.biodata.models.pedigree.Pedigree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;

import java.util.List;

/**
 * @author Diego Poggioli
 *
 * Tasklet that parse and load a PED file into Mongo
 *
 * PED specs
 * http://pngu.mgh.harvard.edu/~purcell/plink/data.shtml#ped
 *
 * TODO: only reading for now, to be completed..
 *
 */
@Component
@StepScope
@Import({JobOptions.class})
public class PedLoaderStep implements Tasklet {
    private static final Logger logger = LoggerFactory.getLogger(PedLoaderStep.class);

    @Autowired
    private JobOptions jobOptions;

    private Pedigree pedigree;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        PedigreeReader pedigreeReader = new PedigreePedReader(jobOptions.getPipelineOptions().getString("input.pedigree"));
        pedigreeReader.open();
        pedigree = pedigreeReader.read().get(0);
        pedigreeReader.close();

        return RepeatStatus.FINISHED;
    }

    public Pedigree getPedigree() {
        return pedigree;
    }

}
