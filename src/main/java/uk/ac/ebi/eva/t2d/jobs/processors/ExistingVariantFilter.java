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
package uk.ac.ebi.eva.t2d.jobs.processors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;
import uk.ac.ebi.eva.t2d.services.T2dService;

/**
 * Processor to convert from Variant to EnsemblVariant and filter those that exist already on the database
 */
public class ExistingVariantFilter implements ItemProcessor<Variant, EnsemblVariant> {

    @Autowired
    private T2dService service;


    @Override
    public EnsemblVariant process(Variant variant) throws Exception {
        if (service.exists(variant)) {
            // Variant already exists, skip annotation and loading
            return null;
        } else {
            return new EnsemblVariant(variant.getChromosome(), variant.getStart(), variant.getEnd(),
                    variant.getReference(), variant.getAlternate());
        }
    }
}
