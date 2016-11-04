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
import java.util.List;

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

    /**
     * We need to fill the samples in the VariantSource, parsing the header before using the factory inside the mapper
     */
    @Override
    protected void doOpen() throws Exception {
        VCFFileReader reader = new VCFFileReader(file, false);
        VCFHeader fileHeader = reader.getFileHeader();

//        source.addMetadata("fileformat", fileHeader.get); // TODO: not in fileHeader??
        source.addMetadata("INFO", fileHeader.getInfoHeaderLines()); // TODO: extract the fields? like id, number, type, description...
        source.addMetadata("FILTER", fileHeader.getFilterLines());
        source.addMetadata("FORMAT", fileHeader.getFormatHeaderLines());
        source.addMetadata("contig", fileHeader.getContigLines());
        for (VCFHeaderLine vcfHeaderLine : fileHeader.getOtherHeaderLines()) {
            source.addMetadata(vcfHeaderLine.getKey(), vcfHeaderLine.getValue());
        }

        source.setSamples(fileHeader.getGenotypeSamples());
        super.doOpen();
    }
}
