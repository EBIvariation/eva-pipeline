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
package uk.ac.ebi.eva.pipeline.io.mappers;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantVcfFactory;
import org.springframework.batch.item.file.LineMapper;

import java.util.List;

/**
 * Maps a String (in VCF format) to a list of variants.
 *
 * The actual implementation is reused from {@link VariantVcfFactory}.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class VcfLineMapper implements LineMapper<List<Variant>> {
    private VariantSource source;
    private VariantVcfFactory factory;

    public VcfLineMapper(VariantSource source) {
        this.source = source;
        factory = new VariantVcfFactory();
    }

    @Override
    public List<Variant> mapLine(String line, int lineNumber) throws Exception {
        return factory.create(source, line);
    }
}
