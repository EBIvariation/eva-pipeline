package uk.ac.ebi.eva.pipeline.configuration;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.pipeline.parameters.ChunkSizeParameters;

@Configuration
public class ChunkSizeCompletionPolicyConfiguration {

    @Bean
    @StepScope
    public SimpleCompletionPolicy chunkSizecompletionPolicy(ChunkSizeParameters chunkSizeParameters) {
        return new SimpleCompletionPolicy(chunkSizeParameters.getChunkSize());
    }

}
