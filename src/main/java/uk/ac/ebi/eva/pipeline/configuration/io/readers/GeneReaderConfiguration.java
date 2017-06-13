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

package uk.ac.ebi.eva.pipeline.configuration.io.readers;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.pipeline.io.readers.GeneReader;
import uk.ac.ebi.eva.pipeline.model.FeatureCoordinates;
import uk.ac.ebi.eva.pipeline.parameters.InputParameters;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.GENE_READER;

/**
 * Configuration to inject GeneReader as in the pipeline.
 */
@Configuration
public class GeneReaderConfiguration {

    @Bean(GENE_READER)
    @StepScope
    public ItemStreamReader<FeatureCoordinates> geneReader(InputParameters inputParameters) {
        return new GeneReader(inputParameters.getGtf());
    }

}
