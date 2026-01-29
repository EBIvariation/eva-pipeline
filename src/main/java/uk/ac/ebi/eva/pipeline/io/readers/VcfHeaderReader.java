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

import com.google.common.base.Splitter;
import org.opencb.biodata.formats.variant.vcf4.Vcf4;
import org.opencb.biodata.formats.variant.vcf4.VcfAlternateHeader;
import org.opencb.biodata.formats.variant.vcf4.VcfFilterHeader;
import org.opencb.biodata.formats.variant.vcf4.VcfFormatHeader;
import org.opencb.biodata.formats.variant.vcf4.VcfInfoHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import uk.ac.ebi.eva.pipeline.runner.exceptions.FileFormatException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

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
    private static final Logger logger = LoggerFactory.getLogger(VcfHeaderReader.class);
    /**
     * The header of the VCF can be retrieved using `source.getMetadata().get(VARIANT_FILE_HEADER_KEY)`.
     */
    public static final String VARIANT_FILE_HEADER_KEY = "header";

    private boolean readAlreadyDone;

    private String fileId;

    private String fileName;

    private String studyId;

    private String studyName;

    private StudyType type;

    private Aggregation aggregation;

    private LocalDate date;

    private Map<String, Integer> samplesPosition;

    private Map<String, Object> metadata;

    private Resource resource;

    private BufferedReader reader;

    private Vcf4 vcf4;

    public VcfHeaderReader(File file,
                           String fileId,
                           String studyId,
                           String studyName,
                           StudyType type,
                           Aggregation aggregation) {
        this.resource = new FileSystemResource(file);
        this.fileName = file.getName();
        this.fileId = fileId;
        this.studyId = studyId;
        this.studyName = studyName;
        this.type = type;
        this.aggregation = aggregation;
        this.readAlreadyDone = false;
        metadata = new HashMap<>();
        samplesPosition = new HashMap<>();
    }

    @Override
    public void setResource(Resource resource) {
        this.resource = resource;
        this.fileName = resource.getFilename();
    }

    /**
     * The ItemReader interface requires a null to be returned after all the elements are read, and we will just
     * read one VariantSourceEntity from a VCF
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
        if (reader == null) {
            throw new IllegalStateException("The BufferedReader should be opened before reading");
        }

        try {
            processHeader();
        } catch (Exception ex) {
            logger.error("There was an error processing the File for reading the VCF Header", ex);
        }

        List<String> sampleNames = new ArrayList<>(vcf4.getSampleNames());
        HashSet<String> uniqueSampleNames = new HashSet<>(sampleNames.size());
        List<String> duplicateSampleNames = new ArrayList<>();

        for (String sample : sampleNames) {
            boolean isDuplicate = !uniqueSampleNames.add(sample);
            if (isDuplicate) {
                duplicateSampleNames.add(sample);
            }
        }
        if (!duplicateSampleNames.isEmpty()) {
            throw new DuplicateSamplesFoundException(duplicateSampleNames);
        }

        VariantSource source = new VariantSource(fileId, fileName, studyId, studyName, type, aggregation, null, samplesPosition, metadata, null);
        return new VariantSourceMongo(source);
    }

    private void processHeader() throws IOException, FileFormatException {
        boolean header = false;
        String line;
        vcf4 = new Vcf4();
        while ((line = reader.readLine()) != null && line.startsWith("#")) {
            if (line.startsWith("##fileformat")) {
                if (line.split("=").length > 1) {
                    vcf4.setFileFormat(line.split("=")[1].trim());
                } else {
                    throw new FileFormatException("");
                }

            } else if (line.startsWith("##INFO")) {
                VcfInfoHeader vcfInfo = new VcfInfoHeader(line);
                vcf4.getInfo().put(vcfInfo.getId(), vcfInfo);

            } else if (line.startsWith("##FILTER")) {
                VcfFilterHeader vcfFilter = new VcfFilterHeader(line);
                vcf4.getFilter().put(vcfFilter.getId(), vcfFilter);

            } else if (line.startsWith("##FORMAT")) {
                VcfFormatHeader vcfFormat = new VcfFormatHeader(line);
                vcf4.getFormat().put(vcfFormat.getId(), vcfFormat);

            } else if (line.startsWith("##ALT")) {
                VcfAlternateHeader vcfAlternateHeader = new VcfAlternateHeader(line);
                vcf4.getAlternate().put(vcfAlternateHeader.getId(), vcfAlternateHeader);

            } else if (line.startsWith("#CHROM")) {
                List<String> headerLine = Splitter.on("\t").splitToList(line.replace("#", ""));
                vcf4.setHeaderLine(headerLine);
                header = true;

            } else {
                String[] fields = line.replace("#", "").split("=", 2);
                vcf4.addMetaInformation(fields[0], fields[1]);
            }
        }
        if (!header) {
            throw new IOException("VCF lacks a header line (the one starting with \"#CHROM\")");
        }
        metadata.put(VARIANT_FILE_HEADER_KEY, vcf4.buildHeader().toString());
        metadata.put("fileformat", vcf4.getFileFormat());
        metadata.put("INFO", vcf4.getInfo().values());
        metadata.put("FILTER", vcf4.getFilter().values());
        metadata.put("FORMAT", vcf4.getFormat().values());
        metadata.put("ALT", vcf4.getAlternate().values());
        for (Map.Entry<String, List<String>> otherMeta : vcf4.getMetaInformation().entrySet()) {
            metadata.put(otherMeta.getKey(), otherMeta.getValue());
        }
        samplesPosition.putAll(vcf4.getSamples());
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        readAlreadyDone = false;
        if (resource == null) {
            throw new ItemStreamException("Resource is not provided");
        }
        try {
            File file = resource.getFile();
            if (file.getName().endsWith(".gz")) {
                this.reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
            } else {
                this.reader = Files.newBufferedReader(file.toPath(), Charset.defaultCharset());
            }
        } catch (IOException e) {
            throw new ItemStreamException("Invalid Resource, File does not exist");
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
    }

    public void close() {
        try {
            reader.close();
        } catch (IOException ex) {
            logger.error("There was an error closing the VCFHeaderReader", ex);
        }
    }
}
