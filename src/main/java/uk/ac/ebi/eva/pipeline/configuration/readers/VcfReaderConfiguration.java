/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.configuration.readers;

import org.opencb.biodata.models.variant.VariantSource;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.io.readers.AggregatedVcfReader;
import uk.ac.ebi.eva.pipeline.io.readers.UnwindingItemStreamReader;
import uk.ac.ebi.eva.pipeline.io.readers.VcfReader;
import uk.ac.ebi.eva.pipeline.parameters.InputParameters;

import java.io.IOException;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_READER;

/**
 * Configuration to inject a VcfReader as a Variant Reader bean.
 */
@Configuration
public class VcfReaderConfiguration {

    @Bean(VARIANT_READER)
    @StepScope
    public ItemStreamReader<Variant> unwindingReader(VcfReader vcfReader) throws Exception {
        return new UnwindingItemStreamReader<>(vcfReader);
    }

    /**
     * The aggregation type is passed so that spring won't cache the instance of VcfReader if it is already built
     * with other aggregation type.
     *
     * @return a VcfReader for the given aggregation type.
     * @throws IOException if the file doesn't exist, because it has to be read to see if it's compressed.
     */
    @Bean
    @StepScope
    public VcfReader vcfReader(InputParameters parameters) throws IOException {
        String fileId = parameters.getVcfId();
        String studyId = parameters.getStudyId();
        VariantSource.Aggregation aggregation = parameters.getVcfAggregation();

        if (VariantSource.Aggregation.NONE.equals(parameters.getVcfAggregation())) {
            return new VcfReader(fileId, studyId, parameters.getVcf());
        } else {
            return new AggregatedVcfReader(fileId, studyId, aggregation, parameters.getVcf());
        }
    }

}
