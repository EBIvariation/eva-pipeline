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
import org.opencb.biodata.models.variant.annotation.ConsequenceType;
import org.opencb.biodata.models.variant.annotation.Score;

import uk.ac.ebi.eva.commons.models.data.VariantAnnotation;
import uk.ac.ebi.eva.test.data.VepOutputContent;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * {@link AnnotationLineMapper}
 * input: an annotation line from VEP
 * output: a VariantAnnotation with at least: consequence types
 */
public class AnnotationLineMapperTest {

    @Test
    public void shouldParseAllDefaultFieldsInVepOutput() throws Exception {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper();
        for (String annotLine : VepOutputContent.vepOutputContent.split("\n")) {
            VariantAnnotation variantAnnotation = lineMapper.mapLine(annotLine, 0);
            assertNotNull(variantAnnotation.getConsequenceTypes());
        }
    }

    @Test
    public void shouldParseAllTranscriptFieldsInVepOutput() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper();
        VariantAnnotation variantAnnotation = lineMapper.mapLine(VepOutputContent.vepOutputContentTranscriptFields, 0);
        List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();

        assertNotNull(consequenceTypes);
        assertTrue(consequenceTypes.size() == 1);

        ConsequenceType consequenceType = consequenceTypes.get(0);

        assertEquals(Integer.valueOf(1), consequenceType.getcDnaPosition());
        assertEquals(Integer.valueOf(4), consequenceType.getCdsPosition());
        assertNull(consequenceType.getAaPosition());
        assertEquals("7-?", consequenceType.getAaChange());
        assertEquals("9-10", consequenceType.getCodon());
    }

    @Test
    public void shouldParseVepOutputWithoutTranscript() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper();
        VariantAnnotation variantAnnotation = lineMapper.mapLine(VepOutputContent.vepOutputContentWithOutTranscript, 0);
        List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();

        assertNotNull(consequenceTypes);
        assertTrue(consequenceTypes.size() == 1);

        ConsequenceType consequenceType = consequenceTypes.get(0);

        assertNotNull(consequenceType.getSoTerms());
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
        VariantAnnotation variantAnnotation = lineMapper
                .mapLine(VepOutputContent.vepOutputContentChromosomeIdWithUnderscore, 0);

        assertEquals("20_1", variantAnnotation.getChromosome());
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void shouldNotParseVepOutputWithMalformedVariantFields() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper();
        lineMapper.mapLine(VepOutputContent.vepOutputContentMalformedVariantFields, 0);
    }

    @Test
    public void shouldParseVepOutputWithExtraFields() {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper();
        VariantAnnotation variantAnnotation = lineMapper.mapLine(VepOutputContent.vepOutputContentWithExtraFields, 0);

        List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();

        assertNotNull(consequenceTypes);
        assertTrue(consequenceTypes.size() == 1);

        ConsequenceType consequenceType = consequenceTypes.get(0);

        List<Score> actualScores = consequenceType.getProteinSubstitutionScores();
        assertNotNull(actualScores);
        assertTrue(actualScores.size() == 2);

        Score expectedSift = new Score(0.07, "Sift", "tolerated");
        Score expectedPolyphen = new Score(0.859, "Polyphen", "possibly_damaging");

        Comparator<Score> scoreComparator = Comparator.comparing(Score::getSource).thenComparing(Score::getDescription)
                .thenComparing(Score::getScore);
        actualScores.sort(scoreComparator);

        assertTrue(Collections.binarySearch(actualScores, expectedSift, scoreComparator) >= 0);
        assertTrue(Collections.binarySearch(actualScores, expectedPolyphen, scoreComparator) >= 0);
    }
}
