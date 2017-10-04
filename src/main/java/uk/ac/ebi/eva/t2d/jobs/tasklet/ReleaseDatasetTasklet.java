package uk.ac.ebi.eva.t2d.jobs.tasklet;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import uk.ac.ebi.eva.t2d.parameters.T2dMetadataParameters;
import uk.ac.ebi.eva.t2d.services.T2dService;

public class ReleaseDatasetTasklet implements Tasklet {

    private final T2dService service;

    private final T2dMetadataParameters metadataParameters;

    public ReleaseDatasetTasklet(T2dService service, T2dMetadataParameters metadataParameters) {
        this.service = service;
        this.metadataParameters = metadataParameters;
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        service.publishDataset(metadataParameters.getDatasetMetadata(), metadataParameters.getSamplesMetadata());
        return RepeatStatus.FINISHED;
    }
}
