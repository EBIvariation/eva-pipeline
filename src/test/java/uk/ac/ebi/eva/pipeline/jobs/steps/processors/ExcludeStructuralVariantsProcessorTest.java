/*
 *
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
 *
 */
package uk.ac.ebi.eva.pipeline.jobs.steps.processors;

import org.junit.BeforeClass;
import org.junit.Test;
import uk.ac.ebi.eva.commons.models.data.Variant;

import java.util.Arrays;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ExcludeStructuralVariantsProcessorTest {

    private String ALT_WITH_SINGLE_BASE = "A";

    private String ALT_WITH_MULTI_BASE = "AT";

    private String ALT_WITH_EMPTY_ALLELE = "";

    private String[] ALTS_WITH_SYMBOLIC_INDEL = new String[]{"<DEL>", "<INS>", "<DUP>", "<INV>", "<CNV>",
            "<DUP:TANDEM>", "<DEL:ME:C1#>", "<INS:ME:C1#>"};

    private String ALT_WITH_SYMBOLIC_ALLELE = "<ID>";

    /**
     * See <a href="https://github.com/EBIvariation/vcf-validator/blob/be6cf8e2b35f2260166c1e6ffa1258a985a99ba3/src/vcf/vcf_v43.ragel#L187">here</a>.
     */
    private String[] ALTS_WITH_BREAK_END_NOTATION = new String[]{
            "G]2:421681]", "]2:421681]G", "]<contig1#>:1234]ATG",
            "G[2:421681[", "[2:421681[G", "[<contig1#>:1234[ATG",
            ".AGT", "AGT."};

    private String ALT_GVCF_NOTATION_FOR_REF_ONLY_RECORDS = "<*>";

    private static ExcludeStructuralVariantsProcessor processor;

    @BeforeClass
    public static void setUp() {
        processor = new ExcludeStructuralVariantsProcessor();
    }

    @Test
    public void altWithSingleBaseAllele() {
        Variant variant = newVariant(ALT_WITH_SINGLE_BASE);
        assertEquals(variant, processor.process(variant));
    }

    private Variant newVariant(String alternate) {
        return new Variant("contig", 1000, 1001, "A", alternate);
    }

    @Test
    public void altWithMultiBaseAllele() {
        Variant variant = newVariant(ALT_WITH_MULTI_BASE);
        assertEquals(variant, processor.process(variant));
    }

    @Test
    public void altWithEmptyAllele() {
        Variant variant = newVariant(ALT_WITH_EMPTY_ALLELE);
        assertEquals(variant, processor.process(variant));
    }

    @Test
    public void altsWithSymbolicIndel() {
        assertTrue(Arrays.stream(ALTS_WITH_SYMBOLIC_INDEL).map(alt -> processor.process(newVariant(alt)))
                .allMatch(Objects::isNull));
    }

    @Test
    public void altWithSymbolicAllele() {
        Variant variant = newVariant(ALT_WITH_SYMBOLIC_ALLELE);
        assertNull(processor.process(variant));
    }

    @Test
    public void altsWithBreakEndNotationAllele() {
        assertTrue(Arrays.stream(ALTS_WITH_BREAK_END_NOTATION).map(alt -> processor.process(newVariant(alt)))
                .allMatch(Objects::isNull));
    }

    @Test
    public void altWithGVCFAsteriskNotation() {
        Variant variant = newVariant(ALT_GVCF_NOTATION_FOR_REF_ONLY_RECORDS);
        assertNull(processor.process(variant));
    }
}
