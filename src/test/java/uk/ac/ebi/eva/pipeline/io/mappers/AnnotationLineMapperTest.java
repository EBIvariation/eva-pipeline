/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.io.mappers;

import org.junit.jupiter.api.Test;
import uk.ac.ebi.eva.commons.mongodb.entities.AnnotationMongo;
import uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.ConsequenceTypeMongo;
import uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.ScoreMongo;
import uk.ac.ebi.eva.test.data.VepOutputContent;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * {@link AnnotationLineMapper}
 * input: an annotation line from VEP
 * output: a Annotation with at least: consequence types
 */
public class AnnotationLineMapperTest {

    private static final String VEP_VERSION = "1";

    private static final String VEP_CACHE_VERSION = "1";

    @Test
    public void shouldParseAllDefaultFieldsInVepOutput() throws Exception {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        for (String annotLine : VepOutputContent.vepOutputContent.split("\n")) {
            AnnotationMongo annotation = lineMapper.mapLine(annotLine, 0);
            assertNotNull(annotation.getConsequenceTypes());
        }
    }

    @Test
    public void shouldParseAllTranscriptFieldsInVepOutput() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        AnnotationMongo annotation = lineMapper.mapLine(VepOutputContent.vepOutputContentTranscriptFields, 0);
        Set<ConsequenceTypeMongo> consequenceTypes = annotation.getConsequenceTypes();

        assertNotNull(consequenceTypes);
        assertEquals(1, consequenceTypes.size());

        ConsequenceTypeMongo consequenceType = consequenceTypes.iterator().next();

        assertEquals(Integer.valueOf(1), consequenceType.getcDnaPosition());
        assertEquals(Integer.valueOf(4), consequenceType.getCdsPosition());
        assertNull(consequenceType.getAaPosition());
        assertEquals("7-?", consequenceType.getAaChange());
        assertEquals("9-10", consequenceType.getCodon());
    }

    @Test
    public void shouldParseVepOutputWithoutTranscript() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        AnnotationMongo annotation = lineMapper.mapLine(VepOutputContent.vepOutputContentWithOutTranscript, 0);
        Set<ConsequenceTypeMongo> consequenceTypes = annotation.getConsequenceTypes();

        assertNotNull(consequenceTypes);
        assertEquals(1, consequenceTypes.size());

        ConsequenceTypeMongo consequenceType = consequenceTypes.iterator().next();

        assertNotNull(consequenceType.getSoAccessions());
        assertNull(consequenceType.getcDnaPosition());
        assertNull(consequenceType.getCdsPosition());
        assertNull(consequenceType.getAaPosition());
        assertNull(consequenceType.getAaChange());
        assertNull(consequenceType.getCodon());
    }

    @Test
    public void shouldNotParseVepOutputWithMalformedCoordinates() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> lineMapper.mapLine(VepOutputContent.vepOutputContentMalformedCoordinates, 0));
    }

    @Test
    public void shouldParseVepOutputWithChromosomeIdWithUnderscore() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        AnnotationMongo annotation = lineMapper
                .mapLine(VepOutputContent.vepOutputContentChromosomeIdWithUnderscore, 0);

        assertEquals("20_1", annotation.getChromosome());
    }

    @Test
    public void shouldNotParseVepOutputWithMalformedVariantFields() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> lineMapper.mapLine(VepOutputContent.vepOutputContentMalformedVariantFields, 0));
    }

    @Test
    public void shouldParseVepOutputWithExtraFields() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        AnnotationMongo annotation = lineMapper.mapLine(VepOutputContent.vepOutputContentWithExtraFieldsSingleAnnotation, 0);

        Set<ConsequenceTypeMongo> consequenceTypes = annotation.getConsequenceTypes();

        assertNotNull(consequenceTypes);
        assertEquals(1, consequenceTypes.size());

        ConsequenceTypeMongo consequenceType = consequenceTypes.iterator().next();

        ScoreMongo polyphen = consequenceType.getPolyphen();
        ScoreMongo sifts = consequenceType.getSift();

        assertNotNull(polyphen);
        assertNotNull(sifts);

        ScoreMongo expectedSift = new ScoreMongo(0.07, "tolerated");
        ScoreMongo expectedPolyphen = new ScoreMongo(0.859, "possibly_damaging");

        assertEquals(expectedSift, sifts);
        assertEquals(expectedPolyphen, polyphen);
    }

    @Test
    public void testChangeRefAltToUpperCase() {
        String vepOutputContent = "20_63351_a/g\t20:63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs181305519\tDISTANCE=4540;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=G:0.0005;AFR_MAF=G:0.0020;polyphen=possibly_damaging(0.859);sift=tolerated(0.07)";
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        String annotLine = vepOutputContent.split("\n")[0];
        AnnotationMongo annotation = lineMapper.mapLine(annotLine, 0);
        assertEquals("20_63351_A_G", annotation.buildVariantId());
    }

    @Test
    public void shouldFailOnUnknownSoTermInTranscriptLine() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        String line = "20_63351_A/G\t20:63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tnot_a_real_so_term\t-\t-\t-\t-\t-\t-";

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> lineMapper.mapLine(line, 0));
        assertTrue(exception.getMessage().contains("not_a_real_so_term"));
    }

    @Test
    public void shouldFailOnUnknownSoTermInNonTranscriptLine() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        String line = "20_60343_G/A\t20:60343\tA\t-\t-\t-\tnot_a_real_so_term\t-\t-\t-\t-\t-\t-";

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> lineMapper.mapLine(line, 0));
        assertTrue(exception.getMessage().contains("not_a_real_so_term"));
    }

    @Test
    public void shouldFailWhenOnlyOneOfSeveralSoTermsIsUnknown() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        String line = "20_63351_A/G\t20:63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tintron_variant,not_a_real_so_term,splice_region_variant\t-\t-\t-\t-\t-\t-";

        assertThrows(IllegalArgumentException.class, () -> lineMapper.mapLine(line, 0));
    }

    @Test
    public void shouldMapMultipleKnownSoTermsWithoutNulls() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        String line = "20_63351_A/G\t20:63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tintron_variant,splice_region_variant\t-\t-\t-\t-\t-\t-";

        AnnotationMongo annotation = lineMapper.mapLine(line, 0);
        ConsequenceTypeMongo consequenceType = annotation.getConsequenceTypes().iterator().next();

        Set<Integer> soAccessions = consequenceType.getSoAccessions();
        assertEquals(new HashSet<>(Arrays.asList(1627, 1630)), soAccessions);
        assertFalse(soAccessions.contains(null));
    }
}
