package embl.ebi.variation.eva.pipeline.jobs;

import embl.ebi.variation.eva.pipeline.VariantJobsArgs;
import org.opencb.datastore.core.ObjectMap;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by diego on 20/05/2016.
 */
@Configuration
public class VariantJobArgsConfig {

    @Bean(initMethod = "loadArgs")
    public VariantJobsArgs variantJobsArgs(){
        return new VariantJobsArgs();
    }

    @Bean
    public ObjectMap variantOptions(){
        return variantJobsArgs().getVariantOptions();
    }

    @Bean
    public ObjectMap pipelineOptions(){
        return variantJobsArgs().getPipelineOptions();
    }


}
