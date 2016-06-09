/*
 * Copyright 2015 EMBL - European Bioinformatics Institute
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
package embl.ebi.variation.eva.pipeline.jobs;

import embl.ebi.variation.eva.VariantJobsArgs;
import org.opencb.datastore.core.ObjectMap;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Diego Poggioli &lt;diego@ebi.ac.uk&gt;
 *
 *  Configuration class to initialize and hold job arguments:
 *  - variantOptions: will be used in opencga
 *  - pipelineOptions: will be used by the pipeline
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
