package uk.ac.ebi.eva.test.t2d.configuration.readers;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.t2d.jobs.readers.TsvReader;
import uk.ac.ebi.eva.t2d.parameters.T2dTsvParameters;

import java.io.File;
import java.io.IOException;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_SAMPLES_READER;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

@Configuration
@Profile(Application.T2D_PROFILE)
public class SamplesReaderConfiguration {

    @Bean(T2D_SAMPLES_READER)
    @StepScope
    public TsvReader samplesReader(T2dTsvParameters samplesParameters) throws IOException {
        TsvReader tsvReader = new TsvReader();
        tsvReader.setResource(getResource(new File(samplesParameters.getSamplesFile())));
        return tsvReader;
    }

}
