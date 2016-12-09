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

import org.opencb.biodata.models.variant.VariantSource;
import org.springframework.batch.item.file.LineMapper;

import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.readers.VariantVcfFactory;

import java.util.List;

import static org.junit.Assert.assertNotNull;

/**
 * Maps a String (in VCF format) to a list of variants.
 * <p>
 * The actual implementation is reused from {@link VariantVcfFactory}.
 */
public class VcfLineMapper implements LineMapper<List<Variant>> {
    private final VariantSource source;

    private final VariantVcfFactory factory;

    public VcfLineMapper(VariantSource source) {
        if (!VariantSource.Aggregation.NONE.equals(source.getAggregation())) {
            throw new IllegalArgumentException(
                    this.getClass().getSimpleName() + " should be used to read genotyped VCFs only, " +
                            "but the VariantSource.Aggregation set to " + source.getAggregation().toString());
        }
        this.source = source;
        this.factory = new VariantVcfFactory();
    }

    @Override
    public List<Variant> mapLine(String line, int lineNumber) {
        assertNotNull(this.getClass().getSimpleName() + " should be used to read genotyped VCFs only," +
                              " (hint: set VariantSource.Aggregation to NONE)",
                      factory);
        return factory.create(source, line);
    }
}
