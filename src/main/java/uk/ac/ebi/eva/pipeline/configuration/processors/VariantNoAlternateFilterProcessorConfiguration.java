/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.configuration.processors;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.jobs.steps.processors.VariantNoAlternateFilterProcessor;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.VARIANT_NO_ALTERNATE_FILTER_PROCESSOR;

/**
 * Configuration to inject {@link VariantNoAlternateFilterProcessor} objects.
 */
@Configuration
public class VariantNoAlternateFilterProcessorConfiguration {

    @Bean(VARIANT_NO_ALTERNATE_FILTER_PROCESSOR)
    @StepScope
    public ItemProcessor<Variant, Variant> variantNoAlternateFilterProcessor() {
        return new VariantNoAlternateFilterProcessor();
    }

}
