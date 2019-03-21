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

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.core.io.Resource;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.io.mappers.VcfLineMapper;
import uk.ac.ebi.eva.utils.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * VCF file reader.
 * <p>
 * This Reader uses a {@link VcfLineMapper} to parse each line.
 * <p>
 * It receives a VariantSource (which needs to be completely filled, including the samples, possibly by
 * {@link VcfHeaderReader}), and a file (compressed or not).
 */
public class VcfReader extends FlatFileItemReader<List<Variant>> {

    public VcfReader(String fileId, String studyId, String file)
            throws IOException {
        this(fileId, studyId, new File(file));
    }

    public VcfReader(String fileId, String studyId, File file)
            throws IOException {
        this(new VcfLineMapper(fileId, studyId), file);
    }

    public VcfReader(LineMapper<List<Variant>> lineMapper, File file) throws IOException {
        Resource resource = FileUtils.getResource(file);
        setResource(resource);
        setLineMapper(lineMapper);
    }

}
