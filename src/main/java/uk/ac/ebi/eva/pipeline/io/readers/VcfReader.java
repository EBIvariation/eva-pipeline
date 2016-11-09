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

import htsjdk.variant.vcf.VCFCompoundHeaderLine;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFContigHeaderLine;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFFilterHeaderLine;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import htsjdk.variant.vcf.VCFSimpleHeaderLine;
import org.opencb.biodata.formats.variant.vcf4.VcfAlternateHeader;
import org.opencb.biodata.formats.variant.vcf4.VcfFilterHeader;
import org.opencb.biodata.formats.variant.vcf4.VcfFormatHeader;
import org.opencb.biodata.formats.variant.vcf4.VcfInfoHeader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import uk.ac.ebi.eva.pipeline.io.GzipLazyResource;
import uk.ac.ebi.eva.pipeline.io.mappers.VcfLineMapper;
import uk.ac.ebi.eva.utils.CompressionHelper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * VCF file reader.
 * <p>
 * This Reader uses a {@link VcfLineMapper} to parse each line, and {@link VCFFileReader} from htslib to fill the
 * VariantSource with metadata from the VCF header.
 * <p>
 * It doesn't matter if the file is compressed or not.
 */
public class VcfReader extends FlatFileItemReader<List<Variant>> {
    /**
     * You can retrieve the header of the VCF like this: `source.getMetadata().get(VARIANT_FILE_HEADER_KEY)`,
     * but why "variantFileHeader"? we are using the converter
     * {@link org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantSourceConverter}, which requires it, and will
     * store it in mongo as "header".
     */
    public static final String VARIANT_FILE_HEADER_KEY = "variantFileHeader";

    private final VariantSource source;

    private final File file;

    public VcfReader(VariantSource source, File file) throws IOException {
        this.source = source;
        this.file = file;
        Resource resource;
        if (CompressionHelper.isGzip(file)) {
            resource = new GzipLazyResource(file);
        } else {
            resource = new FileSystemResource(file);
        }
        setResource(resource);
        setLineMapper(new VcfLineMapper(source));
    }

    public VcfReader(VariantSource source, String file) throws IOException {
        this(source, new File(file));
    }

    @Override
    protected void doOpen() throws Exception {
        prepareVariantSource();
        super.doOpen();
    }

    /**
     * Before using the factory inside the mapper, we need to fill some attributes from the header:
     * <ul>
     * <li>common vcf fields (FORMAT, INFO, ALT, FILTER, contig)</li>
     * <li>other fields (maybe custom fields from users: reference, source...)</li>
     * <li>the sample names.</li>
     * <li>The full header string.</li>
     * </ul>
     * <p>
     * We get the fields with htsjdk to parse the header. As some tags will appear more than once (INFO, contig, ALT...)
     * we store a map, where the key is the field tag, and the value is a list of lines:
     * <p>
     * {@code INFO -> [{ "id" : "CIEND", "number" : "2", "type" : "Integer", "description" : "Confidence..." }, ... ]}
     * <p>
     * We are breaking retrocompatibility here, since the previous structure was wrong. In fields that are different but
     * start with the same key, only the last line was stored, e.g.: {@literal "ALT" : "ID=CN124,Description=...\>"}.
     * Now ALT would map to a list of deconstructed objects: {@code ALT -> [ {id, description}, ...] }
     * <p>
     * Look at the test to see how is this checked.
     */
    private void prepareVariantSource() throws IOException {
        VCFFileReader reader = new VCFFileReader(file, false);
        VCFHeader fileHeader = reader.getFileHeader();

        Map<String, List<VCFHeaderLine>> metadata = new TreeMap<>();
        Set<VCFHeaderLine> metaDataInSortedOrder = fileHeader.getMetaDataInSortedOrder();
        // group the lines by key (INFO, ALT, contig, ...) to a List<VCFHeaderLine>
        for (VCFHeaderLine vcfHeaderLine : metaDataInSortedOrder) {
            if (!metadata.containsKey(vcfHeaderLine.getKey())) {
                metadata.put(vcfHeaderLine.getKey(), new ArrayList<>());
            }
            metadata.get(vcfHeaderLine.getKey()).add(vcfHeaderLine);
        }

        // for each key, transform into biodata's header lines; unless it's an unsupported field and then write strings
        for (Map.Entry<String, List<VCFHeaderLine>> headerLineListEntry : metadata.entrySet()) {
            if (headerLineListEntry.getKey().equals("INFO")) {
                source.getMetadata().put(headerLineListEntry.getKey(), getVcfInfoHeaders(headerLineListEntry));
            } else if (headerLineListEntry.getKey().equals("FILTER")) {
                source.getMetadata().put(headerLineListEntry.getKey(), getVcfFilterHeaders(headerLineListEntry));
            } else if (headerLineListEntry.getKey().equals("FORMAT")) {
                source.getMetadata().put(headerLineListEntry.getKey(), getVcfFormatHeaders(headerLineListEntry));
            } else if (headerLineListEntry.getKey().equals("ALT")) {
                source.getMetadata().put(headerLineListEntry.getKey(), getVcfAlternateHeaders(headerLineListEntry));
            } else if (headerLineListEntry.getKey().equals(VCFConstants.CONTIG_HEADER_KEY)) {
                source.getMetadata().put(headerLineListEntry.getKey(), getContigHeaderLines(headerLineListEntry));
            } else {
                source.addMetadata(headerLineListEntry.getKey(), headerLineListEntry.getValue());
            }
        }

        source.setSamples(fileHeader.getGenotypeSamples());
        reader.close();

        source.addMetadata(VARIANT_FILE_HEADER_KEY, getHeader(metaDataInSortedOrder));
    }

    private List<String> getContigHeaderLines(Map.Entry<String, List<VCFHeaderLine>> headerLineListEntry) {
        List<String> vcfHeaders = new ArrayList<>();
        for (VCFHeaderLine vcfHeaderLine : headerLineListEntry.getValue()) {
            if (vcfHeaderLine instanceof VCFContigHeaderLine) {
                VCFContigHeaderLine line = ((VCFContigHeaderLine) vcfHeaderLine);
                String biodataLine = line.getID() + line.getValue();
                vcfHeaders.add(biodataLine);
            }
        }
        return vcfHeaders;
    }

    private List<VcfAlternateHeader> getVcfAlternateHeaders(Map.Entry<String, List<VCFHeaderLine>> headerLineListEntry) {
        List<VcfAlternateHeader> vcfHeaders = new ArrayList<>();
        for (VCFHeaderLine vcfHeaderLine : headerLineListEntry.getValue()) {
            if (vcfHeaderLine instanceof VCFSimpleHeaderLine) {
                VCFSimpleHeaderLine line = ((VCFSimpleHeaderLine) vcfHeaderLine);
                VcfAlternateHeader biodataLine = new VcfAlternateHeader(line.toString());
                vcfHeaders.add(biodataLine);
            }
        }
        return vcfHeaders;
    }

    private List<VcfFormatHeader> getVcfFormatHeaders(Map.Entry<String, List<VCFHeaderLine>> headerLineListEntry) {
        List<VcfFormatHeader> vcfHeaders = new ArrayList<>();
        for (VCFHeaderLine vcfHeaderLine : headerLineListEntry.getValue()) {
            if (vcfHeaderLine instanceof VCFFormatHeaderLine) {
                VCFFormatHeaderLine line = ((VCFFormatHeaderLine) vcfHeaderLine);
                VcfFormatHeader biodataLine = new VcfFormatHeader(line.getID(), getNumber(line),
                                                                  line.getType().toString(),
                                                                  line.getDescription());
                vcfHeaders.add(biodataLine);
            }
        }
        return vcfHeaders;
    }

    private List<VcfFilterHeader> getVcfFilterHeaders(Map.Entry<String, List<VCFHeaderLine>> headerLineListEntry) {
        List<VcfFilterHeader> vcfHeaders = new ArrayList<>();
        for (VCFHeaderLine vcfHeaderLine : headerLineListEntry.getValue()) {
            if (vcfHeaderLine instanceof VCFFilterHeaderLine) {
                VCFFilterHeaderLine line = ((VCFFilterHeaderLine) vcfHeaderLine);
                VcfFilterHeader biodataLine = new VcfFilterHeader(line.toString());
                vcfHeaders.add(biodataLine);
            }
        }
        return vcfHeaders;
    }

    private List<VcfInfoHeader> getVcfInfoHeaders(Map.Entry<String, List<VCFHeaderLine>> headerLineListEntry) {
        List<VcfInfoHeader> vcfHeaders = new ArrayList<>();
        for (VCFHeaderLine vcfHeaderLine : headerLineListEntry.getValue()) {
            if (vcfHeaderLine instanceof VCFInfoHeaderLine) {
                VCFInfoHeaderLine line = ((VCFInfoHeaderLine) vcfHeaderLine);
                VcfInfoHeader biodataLine = new VcfInfoHeader(line.getID(), getNumber(line), line.getType().toString(),
                                                              line.getDescription());
                vcfHeaders.add(biodataLine);
            }
        }
        return vcfHeaders;
    }

    public String getNumber(VCFCompoundHeaderLine line) {
        String number;
        switch (line.getCountType()) {
            case A:
                number = VCFConstants.PER_ALTERNATE_COUNT;
                break;
            case R:
                number = VCFConstants.PER_ALLELE_COUNT;
                break;
            case G:
                number = VCFConstants.PER_GENOTYPE_COUNT;
                break;
            case UNBOUNDED:
                number = VCFConstants.UNBOUNDED_ENCODING_v4;
                break;
            case INTEGER:
                number = Integer.toString(line.getCount());
                break;
            default:
                throw new UnsupportedOperationException(
                        "VcfReader seems outdated (parsing the header), it doesn't support "
                                + line.getCountType() .toString()
                                + " as VCF metadata count specifier (as member of the enum VCFHeaderLineCount)");
        }
        return number;
    }

    /**
     * VCFHeader from htsjdk adds unwanted text in its toString method:
     * This is a replacement for {@link VCFHeader#toString()}
     */
    private String getHeader(Set<VCFHeaderLine> vcfHeaderLines) throws IOException {
        final StringBuilder builder = new StringBuilder();

        for (VCFHeaderLine vcfHeaderLine : vcfHeaderLines) {
            builder.append(vcfHeaderLine).append("\n");
        }
        return builder.toString();
    }
}
