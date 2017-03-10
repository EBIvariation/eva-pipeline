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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.annotation.ConsequenceType;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.annotation.Score;
import org.opencb.biodata.models.variant.annotation.Xref;
import org.springframework.core.convert.converter.Converter;

import uk.ac.ebi.eva.commons.models.data.VariantAnnotation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link VariantAnnotationToDBObjectConverter}
 */
public class VariantAnnotationToDBObjectConverterTest {
    private Converter<VariantAnnotation, DBObject> converter;

    @Before
    public void setUp() throws Exception {
        converter = new VariantAnnotationToDBObjectConverter();
    }

    @Test(expected = IllegalArgumentException.class)
    public void convertNullVariantAnnotationShouldThrowAnException() {
        converter.convert(null);
    }

    @Test
    public void allFieldsOfVariantAnnotationShouldBeConverted() {
        List<String> soAccessionsValues = Arrays.asList("transcript_ablation", "splice_donor_variant");

        Score polyphenScore = new Score(1.0, "Polyphen", "Polyphen description");
        Score siftScore = new Score(1.0, "Sift", "Sift description");

        List<Score> scores = Arrays.asList(polyphenScore, siftScore);

        ConsequenceType consequenceType = new ConsequenceType("geneName", "ensemblGeneId", "ensemblTranscriptId",
                                                              "strand", "biotype", 0, 0, 0, "aaChange", "codon",
                                                              scores, soAccessionsValues);

        List<ConsequenceType> consequenceTypes = Collections.singletonList(consequenceType);

        VariantAnnotation annotation = new VariantAnnotation("1", 1, 2, "A", "T");
        annotation.setConsequenceTypes(consequenceTypes);
        annotation.setHgvs(Arrays.asList("A", "B"));

        annotation.setXrefs(Collections.singletonList(new Xref("OS01G0112100", "ensemblGene")));

        DBObject dbObject = converter.convert(annotation);
        assertNotNull(dbObject);

        //Consequence types
        LinkedList cts = (LinkedList) dbObject.get(AnnotationFieldNames.CONSEQUENCE_TYPE_FIELD);
        assertEquals(1, cts.size());

        BasicDBObject ct = (BasicDBObject) cts.get(0);
        assertEquals(consequenceType.getGeneName(), ct.getString(AnnotationFieldNames.GENE_NAME_FIELD));
        assertEquals(consequenceType.getEnsemblGeneId(), ct.getString(AnnotationFieldNames.ENSEMBL_GENE_ID_FIELD));
        assertEquals(consequenceType.getEnsemblTranscriptId(),
                     ct.getString(AnnotationFieldNames.ENSEMBL_TRANSCRIPT_ID_FIELD));
        assertEquals(consequenceType.getCodon(), ct.getString(AnnotationFieldNames.CODON_FIELD));
        assertEquals(consequenceType.getStrand(), ct.getString(AnnotationFieldNames.STRAND_FIELD));
        assertEquals(consequenceType.getBiotype(), ct.getString(AnnotationFieldNames.BIOTYPE_FIELD));
        assertEquals(consequenceType.getAaChange(), ct.getString(AnnotationFieldNames.AA_CHANGE_FIELD));

        HashSet sos = new HashSet<>((LinkedList) ct.get(AnnotationFieldNames.SO_ACCESSION_FIELD));

        assertEquals(2, sos.size());

        Set<Integer> expectedSoAccessionsNumbers = soAccessionsValues.stream().map(
                ConsequenceTypeMappings.termToAccession::get).collect(Collectors.toSet());

        assertEquals(expectedSoAccessionsNumbers, sos);

        BasicDBObject polyphenField = (BasicDBObject) ct.get(AnnotationFieldNames.POLYPHEN_FIELD);
        BasicDBObject siftField = (BasicDBObject) ct.get(AnnotationFieldNames.SIFT_FIELD);

        assertEquals(polyphenScore.getScore(), polyphenField.getDouble("sc"), 0.001);
        assertEquals(polyphenScore.getDescription(), polyphenField.getString("desc"));
        assertEquals(siftScore.getScore(), siftField.getDouble("sc"), 0.001);
        assertEquals(siftScore.getDescription(), siftField.getString("desc"));

        //Xrefs
        HashSet<BasicDBObject> xrefs = (HashSet) dbObject.get(AnnotationFieldNames.XREFS_FIELD);

        assertTrue(isXrefPresent(xrefs, "OS01G0112100", "ensemblGene"));    //from xrefs in VariantAnnotation obj
        assertTrue(isXrefPresent(xrefs, "ensemblGeneId", "ensemblGene"));   //from ct
        assertTrue(isXrefPresent(xrefs, "geneName", "HGNC"));   //from ct
        assertTrue(isXrefPresent(xrefs, "ensemblTranscriptId", "ensemblTranscript"));   //from ct
    }

    @Test
    public void onlyConsequenceTypesShouldBeConverted() {
        ConsequenceType consequenceType = new ConsequenceType("geneName", "ensemblGeneId", "ensemblTranscriptId",
                                                              "strand", "biotype", 0, 0, 0, "aaChange", "codon",
                                                              new ArrayList<>(), new ArrayList<>());

        List<ConsequenceType> consequenceTypes = Collections.singletonList(consequenceType);

        VariantAnnotation annotation = new VariantAnnotation("1", 1, 2, "A", "T");
        annotation.setConsequenceTypes(consequenceTypes);

        DBObject dbObject = converter.convert(annotation);
        assertNotNull(dbObject);

        //Consequence types
        LinkedList cts = (LinkedList) dbObject.get(AnnotationFieldNames.CONSEQUENCE_TYPE_FIELD);
        assertEquals(1, cts.size());

        //Xrefs
        HashSet<BasicDBObject> xrefs = (HashSet) dbObject.get(AnnotationFieldNames.XREFS_FIELD);

        assertFalse(isXrefPresent(xrefs, "OS01G0112100", "ensemblGene"));    //from xrefs in VariantAnnotation obj
        assertTrue(isXrefPresent(xrefs, "ensemblGeneId", "ensemblGene"));   //from ct
        assertTrue(isXrefPresent(xrefs, "geneName", "HGNC"));   //from ct
        assertTrue(isXrefPresent(xrefs, "ensemblTranscriptId", "ensemblTranscript"));   //from ct
    }

    @Test
    public void onlyXrefsShouldBeConverted() {
        VariantAnnotation annotation = new VariantAnnotation("1", 1, 2, "A", "T");
        annotation.setXrefs(Collections.singletonList(new Xref("OS01G0112100", "ensemblGene")));

        DBObject dbObject = converter.convert(annotation);

        //Consequence types
        assertNull(dbObject.get(AnnotationFieldNames.CONSEQUENCE_TYPE_FIELD));

        //Xrefs
        HashSet<BasicDBObject> xrefs = (HashSet) dbObject.get(AnnotationFieldNames.XREFS_FIELD);

        assertEquals(1, xrefs.size());
        assertTrue(isXrefPresent(xrefs, "OS01G0112100", "ensemblGene"));    //from xrefs in VariantAnnotation obj
    }

    private boolean isXrefPresent(HashSet<BasicDBObject> xrefs, String id, String src) {
        for (BasicDBObject xref : xrefs) {
            if (xref.getString("id").equals(id) && xref.getString("src").equals(src)) {
                return true;
            }
        }
        return false;
    }
}
