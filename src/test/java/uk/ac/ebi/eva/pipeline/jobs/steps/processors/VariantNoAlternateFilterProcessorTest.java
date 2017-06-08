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

import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.commons.models.data.Variant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;


public class VariantNoAlternateFilterProcessorTest {

    private VariantNoAlternateFilterProcessor processor;

    @Before
    public void setUp() {
        processor = new VariantNoAlternateFilterProcessor();
    }

    @Test
    public void shouldAcceptSnv() throws Exception {
        Variant input = new Variant("1", 1000, 1000, "A", "C");
        Variant output = processor.process(input);
        assertEquals(Variant.VariantType.SNV, input.getType());
        assertNotNull(output);
        assertEquals(input, output);
    }

    @Test
    public void shouldAcceptMnv() throws Exception {
        Variant input = new Variant("1", 1000, 1002, "ATG", "CGT");
        Variant output = processor.process(input);
        assertEquals(Variant.VariantType.MNV, input.getType());
        assertNotNull(output);
        assertEquals(input, output);
    }

    @Test
    public void shouldAcceptIndel() throws Exception {
        Variant input = new Variant("1", 1000, 1001, "AT", "T");
        Variant output = processor.process(input);
        assertEquals(Variant.VariantType.INDEL, input.getType());
        assertNotNull(output);
        assertEquals(input, output);
    }

    @Test
    public void shouldAcceptSv() throws Exception {
        Variant input = new Variant("1", 1000, 1059,
                                    "ATATATATATATATATATATATATATATATATATATATATATATATATATATATATATAT", "T");
        Variant output = processor.process(input);
        assertEquals(Variant.VariantType.SV, input.getType());
        assertNotNull(output);
        assertEquals(input, output);
    }

    @Test
    public void shouldRejectNoAlternateVariants() throws Exception {
        Variant input = new Variant("1", 1000, 1000, "A", ".");
        Variant output = processor.process(input);
        assertEquals(Variant.VariantType.NO_ALTERNATE, input.getType());
        assertNull(output);
    }

}