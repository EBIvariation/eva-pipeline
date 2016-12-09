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
import uk.ac.ebi.eva.commons.readers.VariantAggregatedVcfFactory;
import uk.ac.ebi.eva.commons.readers.VariantVcfEVSFactory;
import uk.ac.ebi.eva.commons.readers.VariantVcfExacFactory;
import uk.ac.ebi.eva.commons.readers.VariantVcfFactory;

import java.util.List;

import static org.junit.Assert.assertNotNull;

/**
 * Maps a String (in VCF format, with aggregated samples) to a list of variants.
 * <p>
 * The actual implementation is reused from {@link VariantVcfFactory}.
 */
public class AggregatedVcfLineMapper implements LineMapper<List<Variant>> {
    private final VariantSource source;

    private VariantVcfFactory factory;

    public AggregatedVcfLineMapper(VariantSource source) {
        switch (source.getAggregation()) {
            case EVS:
                factory = new VariantVcfEVSFactory();
                break;
            case EXAC:
                factory = new VariantVcfExacFactory();
                break;
            case BASIC:
                factory = new VariantAggregatedVcfFactory();
                break;
            case NONE:
                throw new IllegalArgumentException(
                        this.getClass().getSimpleName() + " should be used to read aggregated VCFs only, " +
                                "but the VariantSource.Aggregation is set to NONE");
        }
        this.source = source;
    }

    @Override
    public List<Variant> mapLine(String line, int lineNumber) throws Exception {
        assertNotNull(this.getClass().getSimpleName() + " should be used to read aggregated VCFs only " +
                              "(hint: do not set VariantSource.Aggregation to NONE)",
                      factory);
        return factory.create(source, line);
    }
}
