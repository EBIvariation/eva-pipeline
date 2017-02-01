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
package uk.ac.ebi.eva.pipeline.io.readers;

import org.opencb.biodata.formats.pedigree.io.PedigreePedReader;
import org.opencb.biodata.formats.pedigree.io.PedigreeReader;
import org.opencb.biodata.models.pedigree.Pedigree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;

/**
 * ItemReader that parses a PED file
 * <p>
 * PED specs
 * http://pngu.mgh.harvard.edu/~purcell/plink/data.shtml#ped
 */
public class PedReader implements ItemReader<Pedigree> {
    private static final Logger logger = LoggerFactory.getLogger(PedReader.class);

    private String pedigreePath;

    public PedReader(String pedigreePath) {
        this.pedigreePath = pedigreePath;
    }

    /**
     * Maybe we should return null after the first call? this reader is intended to read just one element.
     */
    @Override
    public Pedigree read() throws Exception {
        PedigreeReader pedigreeReader = new PedigreePedReader(pedigreePath);
        if (!pedigreeReader.open()) {
            throw new Exception("Couldn't open file " + pedigreePath);
        }
        Pedigree pedigree = pedigreeReader.read().get(0);
        pedigreeReader.close();

        return pedigree;
    }
}
