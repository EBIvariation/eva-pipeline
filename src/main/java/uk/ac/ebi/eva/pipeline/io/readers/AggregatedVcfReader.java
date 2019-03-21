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

import org.opencb.biodata.models.variant.VariantSource;

import uk.ac.ebi.eva.pipeline.io.mappers.AggregatedVcfLineMapper;

import java.io.File;
import java.io.IOException;

/**
 * VCF file reader for VCFs without genotypes (aggregated VCFs).
 * <p>
 * This Reader uses a {@link AggregatedVcfReader} to parse each line.
 * <p>
 * It receives a VariantSource (which needs to be completely filled, including the samples, possibly by
 * {@link VcfHeaderReader}), and a file (compressed or not).
 */
public class AggregatedVcfReader extends VcfReader {

    public AggregatedVcfReader(String fileId, String studyId, VariantSource.Aggregation aggregation,
                               String mappingFilePath, File file) throws IOException {
        super(new AggregatedVcfLineMapper(fileId, studyId, aggregation, mappingFilePath), file);
    }
}
