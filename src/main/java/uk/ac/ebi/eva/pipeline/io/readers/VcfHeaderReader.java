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

import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.variant.VariantStudy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import uk.ac.ebi.eva.commons.core.models.Aggregation;
import uk.ac.ebi.eva.commons.core.models.StudyType;
import uk.ac.ebi.eva.commons.core.models.VariantSource;
import uk.ac.ebi.eva.commons.mongodb.entities.VariantSourceMongo;
import uk.ac.ebi.eva.pipeline.runner.exceptions.DuplicateSamplesFoundException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Before providing the VariantSource as argument to a VcfReader (that uses the VariantVcfFactory inside
 * the mapper), we need to fill some attributes from the header:
 * <ul>
 * <li>Common VCF fields (FORMAT, INFO, ALT, FILTER, contig)</li>
 * <li>Other fields (may be custom fields from users: reference, source...)</li>
 * <li>Sample names</li>
 * <li>Full header string</li>
 * </ul>
 * <p>
 * As some tags from the header will appear more than once (INFO, contig, ALT...), they are stored in a Map
 * where the key is the field tag, and the value is a list of lines:
 * <p>
 * {@code INFO -> [{ "id" : "CIEND", "number" : "2", "type" : "Integer", "description" : "Confidence..." }, ... ]}
 * <p>
 * We are breaking retrocompatibility here, since the previous structure was wrong. In fields that are different but
 * start with the same key, only the last line was stored, e.g.: {@literal "ALT" : "ID=CN124,Description=...\>"}.
 * Now ALT would map to a list of deconstructed objects: {@code ALT -> [ {id, description}, ...] }
 * <p>
 * Look at the test to see how is this checked.
 */
public class VcfHeaderReader implements ResourceAwareItemReaderItemStream<VariantSourceMongo> {

    /**
     * The header of the VCF can be retrieved using `source.getMetadata().get(VARIANT_FILE_HEADER_KEY)`.
     */
    public static final String VARIANT_FILE_HEADER_KEY = "header";

    private boolean readAlreadyDone;

    private VariantVcfReader variantReader;

    private Resource resource;

    private final VariantSource source;

    public VcfHeaderReader(File file,
                           String fileId,
                           String studyId,
                           String studyName,
                           StudyType type,
                           Aggregation aggregation) {
        this(file, new VariantSource(file.getName(), fileId, studyId, studyName,
                                     type, aggregation, null, null, null, null));
    }

    public VcfHeaderReader(File file, VariantSource source) {
        this.source = source;
        this.readAlreadyDone = false;
        setResource(new FileSystemResource(file));
    }

    @Override
    public void setResource(Resource resource) {
        this.resource = resource;
    }

    /**
     * The ItemReader interface requires a null to be returned after all the elements are read, and we will just
     * read one VariantSourceMongo from a VCF
     */
    @Override
    public VariantSourceMongo read() throws Exception {
        if (readAlreadyDone) {
            return null;
        } else {
            readAlreadyDone = true;
            return doRead();
        }
    }

    private VariantSourceMongo doRead() throws DuplicateSamplesFoundException {
        if (variantReader == null) {
            throw new IllegalStateException("The method VcfHeaderReader.open() should be called before reading");
        }
        variantReader.pre();
        source.addMetadata(VARIANT_FILE_HEADER_KEY, variantReader.getHeader());

        List<String> sampleNames = new ArrayList<String>(variantReader.getSampleNames());
        HashSet<String> uniqueSampleNames = new HashSet<String>(sampleNames.size());
        List<String> duplicateSampleNames = new ArrayList<String>();

        for (String sample : sampleNames) {
            boolean isDuplicate = !uniqueSampleNames.add(sample);
            if (isDuplicate) {
                duplicateSampleNames.add(sample);
            }
        }
        if (!duplicateSampleNames.isEmpty()) {
            throw new DuplicateSamplesFoundException(duplicateSampleNames);
        }
        return new VariantSourceMongo(source);
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        readAlreadyDone = false;
        checkResourceIsProvided();
        String resourcePath = getResourcePath();
        variantReader = new VariantVcfReader(convertToOpenCBVariantSource(source), resourcePath);
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
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }
    }

    private void doOpen(String path) {
        if (!variantReader.open()) {
            throw new ItemStreamException("Couldn't open file " + path);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
    }

    @Override
    public void close() throws ItemStreamException {
        variantReader.post();
        variantReader.close();
    }

    private org.opencb.biodata.models.variant.VariantSource convertToOpenCBVariantSource(
            VariantSource source) {
        org.opencb.biodata.models.variant.VariantStudy.StudyType studyType =
            org.opencb.biodata.models.variant.VariantStudy.StudyType.fromString(source.getType().toString());
        org.opencb.biodata.models.variant.VariantSource.Aggregation aggregation;
        switch(source.getAggregation()) {
            case EVS:
                aggregation = org.opencb.biodata.models.variant.VariantSource.Aggregation.EVS;
                break;
            case EXAC:
                aggregation = org.opencb.biodata.models.variant.VariantSource.Aggregation.EXAC;
                break;
            case NONE:
                aggregation = org.opencb.biodata.models.variant.VariantSource.Aggregation.NONE;
                break;
            case BASIC:
                aggregation = org.opencb.biodata.models.variant.VariantSource.Aggregation.BASIC;
                break;
            default:
                aggregation = org.opencb.biodata.models.variant.VariantSource.Aggregation.NONE;
                break;
        }
        return new org.opencb.biodata.models.variant.VariantSource(source.getFileName(),
            source.getFileId(), source.getStudyId(), source.getStudyName(), studyType, aggregation);
    }
}
