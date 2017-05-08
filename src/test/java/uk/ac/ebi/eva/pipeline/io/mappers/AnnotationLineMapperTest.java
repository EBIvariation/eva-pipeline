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

import uk.ac.ebi.eva.commons.models.mongo.documents.Annotation;
import uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments.ConsequenceType;
import uk.ac.ebi.eva.commons.models.data.Score;
import uk.ac.ebi.eva.test.data.VepOutputContent;

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

    @Test
    public void shouldParseAllDefaultFieldsInVepOutput() throws Exception {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper();
        for (String annotLine : VepOutputContent.vepOutputContent.split("\n")) {
            Annotation annotation = lineMapper.mapLine(annotLine, 0);
            assertNotNull(annotation.getConsequenceTypes());
        }
    }

    @Test
    public void shouldParseAllTranscriptFieldsInVepOutput() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper();
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
        AnnotationLineMapper lineMapper = new AnnotationLineMapper();
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
        AnnotationLineMapper lineMapper = new AnnotationLineMapper();
        lineMapper.mapLine(VepOutputContent.vepOutputContentMalformedCoordinates, 0);
    }

    @Test
    public void shouldParseVepOutputWithChromosomeIdWithUnderscore() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper();
        Annotation annotation = lineMapper
                .mapLine(VepOutputContent.vepOutputContentChromosomeIdWithUnderscore, 0);

        assertEquals("20_1", annotation.getChromosome());
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void shouldNotParseVepOutputWithMalformedVariantFields() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper();
        lineMapper.mapLine(VepOutputContent.vepOutputContentMalformedVariantFields, 0);
    }

    @Test
    public void shouldParseVepOutputWithExtraFields() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper();
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

        assertEquals(sifts, expectedSift);
        assertEquals(polyphen, expectedPolyphen);
    }
}
