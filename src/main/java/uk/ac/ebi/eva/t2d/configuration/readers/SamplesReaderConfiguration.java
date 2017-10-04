package uk.ac.ebi.eva.t2d.configuration.readers;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.FileSystemResource;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.t2d.jobs.readers.TsvReader;
import uk.ac.ebi.eva.t2d.parameters.T2dTsvParameters;

import java.io.File;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_SAMPLES_READER;

@Configuration
@Profile(Application.T2D_PROFILE)
public class SamplesReaderConfiguration {

    @Bean(T2D_SAMPLES_READER)
    @StepScope
    public TsvReader samplesReader(T2dTsvParameters samplesParameters) {
        TsvReader tsvReader = new TsvReader();
        tsvReader.setResource(new FileSystemResource(new File(samplesParameters.getSamplesFile())));
        return tsvReader;
    }

}
