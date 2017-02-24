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
package uk.ac.ebi.eva.pipeline.io.readers;

import com.google.common.collect.Sets;
import org.junit.Test;
import org.opencb.biodata.models.pedigree.Condition;
import org.opencb.biodata.models.pedigree.Individual;
import org.opencb.biodata.models.pedigree.Pedigree;
import org.opencb.biodata.models.pedigree.Sex;

import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.utils.FileUtils.getResource;

/**
 * Test for {@link PedReader}
 * <p>
 * input: a pedigree file
 * output: a Pedigree when method `.read()` is called.
 */
public class PedReaderTest {
    private static final String PEDIGREE_FILE = "/input-files/ped/pedigree-test-file.ped";

    private static final String MALFORMED_PEDIGREE = "/input-files/ped/malformed-pedigree-test-file.ped";

    @Test
    public void wholePedFileShouldBeParsedIntoPedigree() throws Exception {
        String pedigreePath = getResource(PEDIGREE_FILE).getAbsolutePath();
        PedReader pedReader = new PedReader(pedigreePath);
        pedReader.open(null);
        Pedigree pedigree = pedReader.read();

        //check that Pedigree.Individuals is correctly populated
        assertEquals(4, pedigree.getIndividuals().size());
        Individual individualNA19660 = pedigree.getIndividuals().get("NA19660");
        assertTrue(individualNA19660.getFamily().equals("FAM"));
        assertTrue(individualNA19660.getSex().equals("2"));
        assertEquals(Sex.FEMALE, individualNA19660.getSexCode());
        assertTrue(individualNA19660.getPhenotype().equals("1"));
        assertEquals(Condition.UNAFFECTED, individualNA19660.getCondition());
        assertEquals(2, individualNA19660.getChildren().size());
        assertEquals(Sets.newHashSet("NA19600", "NA19685"),
                individualNA19660.getChildren().stream().map(Individual::getId).collect(Collectors.toSet()));

        //check that Pedigree.Families is correctly populated
        assertEquals(1, pedigree.getFamilies().size());
        assertTrue(pedigree.getFamilies().containsKey("FAM"));
        assertEquals(4, pedigree.getFamilies().get("FAM").size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void missingLastColumnInPedFileShouldThrowsException() throws Exception {
        String pedigreePath = getResource(MALFORMED_PEDIGREE).getAbsolutePath();
        PedReader pedReader = new PedReader(pedigreePath);
        pedReader.open(null);
        pedReader.read();
    }

}
