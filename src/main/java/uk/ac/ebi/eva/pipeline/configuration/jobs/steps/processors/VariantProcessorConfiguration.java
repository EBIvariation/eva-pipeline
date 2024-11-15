/*
 * Copyright 2024 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.pipeline.configuration.jobs.steps.processors;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.jobs.steps.processors.ExcludeStructuralVariantsProcessor;
import uk.ac.ebi.eva.pipeline.jobs.steps.processors.VariantNoAlternateFilterProcessor;

import java.util.Arrays;

import static uk.ac.ebi.eva.pipeline.configuration.BeanNames.COMPOSITE_VARIANT_PROCESSOR;


/**
 * Configuration to inject a VariantProcessor as a bean.
 */
@Configuration
public class VariantProcessorConfiguration {
    @Bean(COMPOSITE_VARIANT_PROCESSOR)
    @StepScope
    public ItemProcessor<Variant, Variant> compositeVariantProcessor(
            VariantNoAlternateFilterProcessor variantNoAlternateFilterProcessor,
            ExcludeStructuralVariantsProcessor excludeStructuralVariantsProcessor) {
        CompositeItemProcessor<Variant, Variant> compositeProcessor = new CompositeItemProcessor<>();
        compositeProcessor.setDelegates(Arrays.asList(variantNoAlternateFilterProcessor,
                excludeStructuralVariantsProcessor));

        return compositeProcessor;
    }

    @Bean
    public ExcludeStructuralVariantsProcessor excludeStructuralVariantsProcessor() {
        return new ExcludeStructuralVariantsProcessor();
    }

    @Bean
    public VariantNoAlternateFilterProcessor variantNoAlternateFilterProcessor() {
        return new VariantNoAlternateFilterProcessor();
    }
}
