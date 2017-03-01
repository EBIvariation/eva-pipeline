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
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.io.IOException;

/**
 * ItemReader that parses a PED file
 * <p>
 * PED specs
 * http://pngu.mgh.harvard.edu/~purcell/plink/data.shtml#ped
 */
public class PedReader implements ResourceAwareItemReaderItemStream<Pedigree> {
    private static final Logger logger = LoggerFactory.getLogger(PedReader.class);

    private boolean readAlreadyDone;

    private PedigreeReader pedigreeReader;

    private Resource resource;

    public PedReader() {
        this.readAlreadyDone = false;
    }

    public PedReader(String pedigreePath) {
        this();
        setResource(new FileSystemResource(pedigreePath));
    }

    @Override
    public void setResource(Resource resource) {
        this.resource = resource;
    }

    /**
     * The ItemReader interface requires a null to be returned after all the elements are read, and we will just
     * read one Pedigree from a ped file.
     */
    @Override
    public Pedigree read() throws Exception {
        if (readAlreadyDone) {
            return null;
        } else {
            readAlreadyDone = true;
            return doRead();
        }
    }

    private Pedigree doRead() {
        if (pedigreeReader == null) {
            throw new IllegalStateException("The method PedReader.open() should be called before reading");
        }
        return pedigreeReader.read().get(0);
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        readAlreadyDone = false;
        checkResourceIsProvided();
        String resourcePath = getResourcePath();
        pedigreeReader = new PedigreePedReader(resourcePath);
        doOpen(resourcePath);
    }

    private void checkResourceIsProvided() {
        if (resource == null) {
            throw new ItemStreamException("Resource was not provided.");
        }
    }

    private String getResourcePath() {
        try {
            return resource.getFile().getAbsolutePath();
        } catch (IOException innerException) {
            throw new ItemStreamException(innerException);
        }
    }

    private void doOpen(String path) {
        if (!pedigreeReader.open()) {
            throw new ItemStreamException("Couldn't open file " + path);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
    }

    @Override
    public void close() throws ItemStreamException {
        pedigreeReader.close();
    }
}
