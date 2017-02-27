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
package uk.ac.ebi.eva.commons.models.converters.data;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.annotation.ConsequenceType;
import org.opencb.biodata.models.variant.annotation.Score;
import org.opencb.biodata.models.variant.annotation.Xref;
import org.springframework.core.convert.converter.Converter;

import uk.ac.ebi.eva.commons.models.data.VariantAnnotation;
import uk.ac.ebi.eva.test.data.VariantAnnotationData;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link DBObjectToVariantAnnotationConverter}
 */
public class DBObjectToVariantAnnotationConverterTest {
    private Converter<DBObject, VariantAnnotation> converter;

    @Before
    public void setUp() throws Exception {
        converter = new DBObjectToVariantAnnotationConverter();
    }

    @Test(expected = IllegalArgumentException.class)
    public void convertNullVariantAnnotationShouldThrowAnException() {
        converter.convert(null);
    }

    @Test
    public void allFieldsOfVariantAnnotationShouldBeConverted() {
        DBObject dbObject = (DBObject) JSON.parse(VariantAnnotationData.VARIANT_ANNOTATION_JSON);

        VariantAnnotation annotation = converter.convert(dbObject);

        //Consequence types
        List<ConsequenceType> consequenceTypeList = annotation.getConsequenceTypes();

        assertTrue(consequenceTypeList.size() == 1);
        ConsequenceType consequenceType = consequenceTypeList.get(0);

        assertEquals("geneName", consequenceType.getGeneName());
        assertEquals("ensemblGeneId", consequenceType.getEnsemblGeneId());
        assertEquals("ensemblTranscriptId", consequenceType.getEnsemblTranscriptId());
        assertEquals("strand", consequenceType.getStrand());
        assertEquals("biotype", consequenceType.getBiotype());
        assertEquals("aaChange", consequenceType.getAaChange());
        assertEquals("codon", consequenceType.getCodon());

        List<Score> actualScores = consequenceType.getProteinSubstitutionScores();

        Score expectedScore1 = new Score(1.0, "Polyphen", "Polyphen description");
        Score expectedScore2 = new Score(1.0, "Sift", "Sift description");

        Comparator<Score> scoreComparator = Comparator.comparing(Score::getSource).thenComparing(Score::getDescription)
                .thenComparing(Score::getScore);
        actualScores.sort(scoreComparator);

        assertTrue(Collections.binarySearch(actualScores, expectedScore1, scoreComparator) >= 0);
        assertTrue(Collections.binarySearch(actualScores, expectedScore2, scoreComparator) >= 0);

        List<ConsequenceType.ConsequenceTypeEntry> consequenceTypeEntries = consequenceType.getSoTerms();

        Comparator<ConsequenceType.ConsequenceTypeEntry> consequenceTypeEntryComparator = Comparator.comparing(
                ConsequenceType.ConsequenceTypeEntry::getSoAccession).thenComparing(
                ConsequenceType.ConsequenceTypeEntry::getSoName);
        consequenceTypeEntries.sort(consequenceTypeEntryComparator);

        ConsequenceType.ConsequenceTypeEntry expectedConsequenceTypeEntry1 = new ConsequenceType.ConsequenceTypeEntry(
                "transcript_ablation", "SO:0001893");
        ConsequenceType.ConsequenceTypeEntry expectedConsequenceTypeEntry2 = new ConsequenceType.ConsequenceTypeEntry(
                "splice_donor_variant", "SO:0001575");

        assertTrue(Collections.binarySearch(consequenceTypeEntries, expectedConsequenceTypeEntry1,
                                            consequenceTypeEntryComparator) >= 0);
        assertTrue(Collections.binarySearch(consequenceTypeEntries, expectedConsequenceTypeEntry2,
                                            consequenceTypeEntryComparator) >= 0);

        //Xrefs
        List<Xref> xrefList = annotation.getXrefs();
        assertTrue(xrefList.size() == 4);

        Xref xref1 = new Xref("OS01G0112100", "ensemblGene");
        Xref xref2 = new Xref("ensemblGeneId", "ensemblGene");
        Xref xref3 = new Xref("geneName", "HGNC");
        Xref xref4 = new Xref("ensemblTranscriptId", "ensemblTranscript");

        Comparator<Xref> xrefComparator = Comparator.comparing(Xref::getId).thenComparing(Xref::getSrc);

        xrefList.sort(xrefComparator);

        assertTrue(Collections.binarySearch(xrefList, xref1, xrefComparator) >= 0);
        assertTrue(Collections.binarySearch(xrefList, xref2, xrefComparator) >= 0);
        assertTrue(Collections.binarySearch(xrefList, xref3, xrefComparator) >= 0);
        assertTrue(Collections.binarySearch(xrefList, xref4, xrefComparator) >= 0);
    }

}
