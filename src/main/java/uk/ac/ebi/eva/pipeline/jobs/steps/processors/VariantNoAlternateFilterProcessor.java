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
package uk.ac.ebi.eva.pipeline.jobs.steps.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.commons.models.data.Variant;

/**
 * Filters out variants reporting no alternate allele.
 */
public class VariantNoAlternateFilterProcessor implements ItemProcessor<Variant, Variant> {

    private static final Logger logger = LoggerFactory.getLogger(VariantNoAlternateFilterProcessor.class);

    @Override
    public Variant process(Variant item) throws Exception {
        if (item.getType() != Variant.VariantType.NO_ALTERNATE) {
            return item;
        } else {
            logger.warn("Variant {}:{}:{}>{} ignored due to no alternate allele reported",
                        item.getChromosome(), item.getStart(), item.getReference(), item.getAlternate());
            return null;
        }
    }
}
