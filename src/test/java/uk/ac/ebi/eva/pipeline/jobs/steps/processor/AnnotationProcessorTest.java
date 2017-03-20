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
package uk.ac.ebi.eva.pipeline.jobs.steps.processor;

import org.junit.Test;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantAnnotation;
import uk.ac.ebi.eva.pipeline.jobs.steps.processors.AnnotationProcessor;
import uk.ac.ebi.eva.pipeline.model.VariantWrapper;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * {@link AnnotationProcessor}
 * input: a DBObject
 * output: a VariantWrapper
 */
public class AnnotationProcessorTest {

    @Test
    public void shouldConvertAllFieldsInVariant() throws Exception {
        Variant variant = new Variant("1", 100, 100, "A", "T");
        VariantWrapper variantWrapper = new VariantWrapper(variant);
        AnnotationProcessor processor = new AnnotationProcessor(600);
        List<VariantAnnotation> variantAnnotations = processor.process(variantWrapper);

        assertEquals(1, variantAnnotations.size());
        VariantAnnotation variantAnnotation = variantAnnotations.get(0);
        assertEquals(variantAnnotation.getChromosome(), variant.getChromosome());
        assertEquals(variantAnnotation.getStart(), variant.getStart());
        assertEquals(variantAnnotation.getEnd(), variant.getEnd());
        assertEquals(variantAnnotation.getReferenceAllele(), variant.getReference());
        assertEquals(variantAnnotation.getAlternativeAllele(), variant.getAlternate());
    }

}
