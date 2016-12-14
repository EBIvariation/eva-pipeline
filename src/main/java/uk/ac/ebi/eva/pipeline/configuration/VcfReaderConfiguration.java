package uk.ac.ebi.eva.pipeline.configuration;

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.io.readers.AggregatedVcfReader;
import uk.ac.ebi.eva.pipeline.io.readers.UnwindingItemStreamReader;
import uk.ac.ebi.eva.pipeline.io.readers.VcfReader;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;

import java.io.IOException;
import java.util.List;

@Configuration
public class VcfReaderConfiguration {

    @Bean
    @StepScope
    public ItemStreamReader<Variant> unwindingReader(VcfReader vcfReader) throws Exception {
        return new UnwindingItemStreamReader<>(vcfReader);
    }

    /**
     * The aggregation type is passed so that spring won't cache the instance of VcfReader if it is already built
     * with other aggregation type.
     *
     * @param aggregationType to decide whether to instantiate a VcfReader or AggregatedVcfReader.
     * @return a VcfReader for the given aggregation type.
     * @throws IOException if the file doesn't exist, because it has to be read to see if it's compressed.
     */
    @Bean
    @StepScope
    public VcfReader vcfReader(@Value("${" + JobParametersNames.INPUT_VCF_AGGREGATION + "}")
                                                       String aggregationType,
                                               JobOptions jobOptions) throws IOException {
        VariantSource.Aggregation aggregation = VariantSource.Aggregation.valueOf(aggregationType);
        if (VariantSource.Aggregation.NONE.equals(aggregation)) {
            return new VcfReader(
                    (VariantSource) jobOptions.getVariantOptions().get(VariantStorageManager.VARIANT_SOURCE),
                    jobOptions.getPipelineOptions().getString(JobParametersNames.INPUT_VCF));
        } else {
            return new AggregatedVcfReader(
                    (VariantSource) jobOptions.getVariantOptions().get(VariantStorageManager.VARIANT_SOURCE),
                    jobOptions.getPipelineOptions().getString(JobParametersNames.INPUT_VCF));
        }
    }

}
