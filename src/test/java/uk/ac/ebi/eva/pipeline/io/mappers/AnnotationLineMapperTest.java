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

import org.junit.Test;

import uk.ac.ebi.eva.commons.models.mongo.entity.Annotation;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.ConsequenceType;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.Score;
import uk.ac.ebi.eva.test.data.VepOutputContent;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
            Annotation annotation = lineMapper.mapLine(annotLine, 0);
            assertNotNull(annotation.getConsequenceTypes());
        }
    }

    @Test
    public void shouldParseAllTranscriptFieldsInVepOutput() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        Annotation annotation = lineMapper.mapLine(VepOutputContent.vepOutputContentTranscriptFields, 0);
        Set<ConsequenceType> consequenceTypes = annotation.getConsequenceTypes();

        assertNotNull(consequenceTypes);
        assertEquals(1, consequenceTypes.size());

        ConsequenceType consequenceType = consequenceTypes.iterator().next();

        assertEquals(Integer.valueOf(1), consequenceType.getcDnaPosition());
        assertEquals(Integer.valueOf(4), consequenceType.getCdsPosition());
        assertNull(consequenceType.getAaPosition());
        assertEquals("7-?", consequenceType.getAaChange());
        assertEquals("9-10", consequenceType.getCodon());
    }

    @Test
    public void shouldParseVepOutputWithoutTranscript() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        Annotation annotation = lineMapper.mapLine(VepOutputContent.vepOutputContentWithOutTranscript, 0);
        Set<ConsequenceType> consequenceTypes = annotation.getConsequenceTypes();

        assertNotNull(consequenceTypes);
        assertEquals(1, consequenceTypes.size());

        ConsequenceType consequenceType = consequenceTypes.iterator().next();

        assertNotNull(consequenceType.getSoAccessions());
        assertNull(consequenceType.getcDnaPosition());
        assertNull(consequenceType.getCdsPosition());
        assertNull(consequenceType.getAaPosition());
        assertNull(consequenceType.getAaChange());
        assertNull(consequenceType.getCodon());
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void shouldNotParseVepOutputWithMalformedCoordinates() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        lineMapper.mapLine(VepOutputContent.vepOutputContentMalformedCoordinates, 0);
    }

    @Test
    public void shouldParseVepOutputWithChromosomeIdWithUnderscore() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        Annotation annotation = lineMapper
                .mapLine(VepOutputContent.vepOutputContentChromosomeIdWithUnderscore, 0);

        assertEquals("20_1", annotation.getChromosome());
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void shouldNotParseVepOutputWithMalformedVariantFields() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        lineMapper.mapLine(VepOutputContent.vepOutputContentMalformedVariantFields, 0);
    }

    @Test
    public void shouldParseVepOutputWithExtraFields() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        Annotation annotation = lineMapper.mapLine(VepOutputContent.vepOutputContentWithExtraFieldsSingleAnnotation, 0);

        Set<ConsequenceType> consequenceTypes = annotation.getConsequenceTypes();

        assertNotNull(consequenceTypes);
        assertEquals(1, consequenceTypes.size());

        ConsequenceType consequenceType = consequenceTypes.iterator().next();

        Score polyphen = consequenceType.getPolyphen();
        Score sifts = consequenceType.getSift();

        assertNotNull(polyphen);
        assertNotNull(sifts);

        Score expectedSift = new Score(0.07, "tolerated");
        Score expectedPolyphen = new Score(0.859, "possibly_damaging");

        assertEquals(expectedSift, sifts);
        assertEquals(expectedPolyphen, polyphen);
    }

    @Test
    public void testChangeRefAltToUpperCase(){
        String vepOutputContent = "20_63351_a/g\t20:63351\tG\tENSG00000178591\tENST00000608838\tTranscript\tupstream_gene_variant\t-\t-\t-\t-\t-\trs181305519\tDISTANCE=4540;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript;GMAF=G:0.0005;AFR_MAF=G:0.0020;polyphen=possibly_damaging(0.859);sift=tolerated(0.07)";
        AnnotationLineMapper lineMapper = new AnnotationLineMapper(VEP_VERSION, VEP_CACHE_VERSION);
        String annotLine = vepOutputContent.split("\n")[0];
        Annotation annotation = lineMapper.mapLine(annotLine, 0);
        assertEquals("20_63351_A_G", annotation.buildVariantId());
    }
}
