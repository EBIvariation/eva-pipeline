package uk.ac.ebi.eva.t2d.configuration.readers;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.t2d.jobs.readers.TableDefinitionReader;
import uk.ac.ebi.eva.t2d.parameters.T2dMetadataParameters;
import uk.ac.ebi.eva.t2d.parameters.T2dTsvParameters;

import java.io.File;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_SAMPLES_DATA_STRUCTURE_READER;

@Configuration
@Profile(Application.T2D_PROFILE)
public class StudySampleDefinitionReaderConfiguration {

    @Bean(T2D_SAMPLES_DATA_STRUCTURE_READER)
    @StepScope
    public TableDefinitionReader samplesDataStructure(T2dMetadataParameters metadataParameters,
                                                      T2dTsvParameters samplesParameters) {
        return new TableDefinitionReader(new File(samplesParameters.getSamplesDefinitionFile()),
                metadataParameters.getSamplesMetadata().getTableName());
    }

}
