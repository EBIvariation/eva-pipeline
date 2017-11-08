package uk.ac.ebi.eva.t2d.configuration.processors;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;
import uk.ac.ebi.eva.t2d.jobs.processors.ExistingVariantFilter;

import static uk.ac.ebi.eva.t2d.BeanNames.EXISTING_VARIANT_FILTER;

@Configuration
public class ExistingVariantFilterConfiguration {

    @Bean(EXISTING_VARIANT_FILTER)
    @StepScope
    public ItemProcessor<Variant, EnsemblVariant> existingVariantFilterConfiguration(){
        return new ExistingVariantFilter();
    }

}
