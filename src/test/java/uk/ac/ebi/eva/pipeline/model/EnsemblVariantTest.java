/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.pipeline.model;

import org.junit.Test;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.io.mappers.VariantVcfFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * VEP input format uses a different end than we. These tests check that we comply with the examples in the VEP
 * documentation at: www.ensembl.org/info/docs/tools/vep/vep_formats.html ("Default" and "VCF" sections)
 */
public class EnsemblVariantTest {

    private static final String FILE_ID = "fid";

    private static final String STUDY_ID = "sid";

    private VariantVcfFactory factory = new VariantVcfFactory();

    @Test
    public void transformInsertionsToEnsembleCoordinates() throws Exception {
        EnsemblVariant insertion;
        insertion = new EnsemblVariant("1", 12601, 12601, "", "C");
        assertEquals(12601, insertion.getStart());
        assertEquals(12600, insertion.getEnd());
        assertEquals("-/C", insertion.getRefAlt());

        insertion = new EnsemblVariant("20", 4, 5, "", "A");
        assertEquals(4, insertion.getStart());
        assertEquals(3, insertion.getEnd());
        assertEquals("-/A", insertion.getRefAlt());
    }

    @Test
    public void transformDeletionToEnsembleCoordinates() throws Exception {
        EnsemblVariant deletion;
        deletion = new EnsemblVariant("1", 12600, 12602, "CGT", "");
        assertEquals(12600, deletion.getStart());
        assertEquals(12602, deletion.getEnd());
        assertEquals("CGT/-", deletion.getRefAlt());

        deletion = new EnsemblVariant("20", 3, 3, "C", "");
        assertEquals(3, deletion.getStart());
        assertEquals(3, deletion.getEnd());
        assertEquals("C/-", deletion.getRefAlt());
    }

    @Test
    public void transformSnvToEnsembleCoordinates() throws Exception {
        EnsemblVariant insertion = new EnsemblVariant("20", 3, 3, "C", "G");
        assertEquals(3, insertion.getStart());
        assertEquals(3, insertion.getEnd());
        assertEquals("C/G", insertion.getRefAlt());
    }

    @Test
    public void transformInsertionFromVcfToEnsemblCoordinates() throws Exception {
        String line = "20\t3\t.\tC\tCA\t.\t.\t.";
        List<Variant> result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(1, result.size());

        int start = result.get(0).getStart();
        String reference = result.get(0).getReference();
        String alternate = result.get(0).getAlternate();

        assertEquals(4, start);
        assertEquals("", reference);
        assertEquals("A", alternate);

        EnsemblVariant insertion = new EnsemblVariant(result.get(0).getChromosome(), start, result.get(0).getEnd(),
                                                      reference, alternate);
        assertEquals(4, insertion.getStart());
        assertEquals(3, insertion.getEnd());
        assertEquals("-/A", insertion.getRefAlt());
    }

    @Test
    public void transformDeletionFromVcfToEnsemblCoordinates() throws Exception {
        String line = "20\t2\t.\tTC\tT\t.\t.\t.";
        List<Variant> result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(1, result.size());

        int start = result.get(0).getStart();
        String reference = result.get(0).getReference();
        String alternate = result.get(0).getAlternate();

        assertEquals(3, start);
        assertEquals("C", reference);
        assertEquals("", alternate);

        EnsemblVariant insertion = new EnsemblVariant(result.get(0).getChromosome(), start, result.get(0).getEnd(),
                                                      reference, alternate);
        assertEquals(3, insertion.getStart());
        assertEquals(3, insertion.getEnd());
        assertEquals("C/-", insertion.getRefAlt());
    }
}
