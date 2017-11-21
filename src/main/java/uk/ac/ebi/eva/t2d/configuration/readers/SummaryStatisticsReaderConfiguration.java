/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.t2d.configuration.readers;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.t2d.jobs.readers.TsvReader;
import uk.ac.ebi.eva.t2d.parameters.T2dTsvParameters;

import java.io.File;
import java.io.IOException;

import static uk.ac.ebi.eva.t2d.BeanNames.T2D_SUMMARY_STATISTICS_READER;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * Bean configuration for a TSV file reader that reads the summary statistics file.
 */
@Configuration
@Profile(Application.T2D_PROFILE)
public class SummaryStatisticsReaderConfiguration {

    @Bean(T2D_SUMMARY_STATISTICS_READER)
    @StepScope
    public TsvReader samplesReader(T2dTsvParameters parameters) throws IOException {
        TsvReader tsvReader = new TsvReader();
        tsvReader.setResource(getResource(new File(parameters.getSummaryStatisticsFile())));
        return tsvReader;
    }

}
