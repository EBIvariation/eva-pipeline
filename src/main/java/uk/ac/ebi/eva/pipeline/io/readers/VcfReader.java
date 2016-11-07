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

import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
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
import java.util.*;

/**
 * VCF file reader.
 *
 * This Reader uses a {@link VcfLineMapper} to parse each line, and {@link VariantVcfReader} to fill the VariantSource.
 *
 * It doesn't matter if the file is compressed or not.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
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
     * - the sample names.
     * - common vcf fields (FORMAT, INFO, ALT, FILTER, contig)
     * - other fields (maybe custom fields from users: reference, source...)
     *
     * We get the fields with htsjdk to parse the header. As some tags will appear more than once (INFO, contig, ALT...)
     * we store a map, where the key is the field tag, and the value is a list of lines:
     *     INFO -> ["##INFO=...ID=CIEND...", "##INFO=...ID=CIPOS..."]
     */
    private void prepareVariantSource() throws IOException {
        VCFFileReader reader = new VCFFileReader(file, false);
        VCFHeader fileHeader = reader.getFileHeader();

        Map<String, List<String>> metadata = new TreeMap<>();
        Set<VCFHeaderLine> metaDataInSortedOrder = fileHeader.getMetaDataInSortedOrder();
        for (VCFHeaderLine vcfHeaderLine : metaDataInSortedOrder) {
            if (!metadata.containsKey(vcfHeaderLine.getKey())) {
                metadata.put(vcfHeaderLine.getKey(), new ArrayList<>());
            }
            metadata.get(vcfHeaderLine.getKey()).add(vcfHeaderLine.toString());
        }

        for (Map.Entry<String, List<String>> stringListEntry : metadata.entrySet()) {
            source.addMetadata(stringListEntry.getKey(), stringListEntry.getValue());
        }
        source.setSamples(fileHeader.getGenotypeSamples());
        reader.close();

        source.addMetadata(VARIANT_FILE_HEADER_KEY, getHeader(metaDataInSortedOrder));
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
