package uk.ac.ebi.eva.pipeline.configuration;

import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.mongodb.core.MongoOperations;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.io.writers.VariantMongoWriter;
import uk.ac.ebi.eva.pipeline.model.converters.data.VariantToMongoDbObjectConverter;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.utils.MongoDBHelper;

@Configuration
public class VariantWriterConfiguration {

    @Bean
    @StepScope
    @Profile(Application.VARIANT_WRITER_MONGO_PROFILE)
    public ItemWriter<Variant> variantMongoWriter(JobOptions jobOptions) throws Exception {
        MongoOperations mongoOperations = MongoDBHelper
                .getMongoOperations(jobOptions.getDbName(), jobOptions.getMongoConnection());

        return new VariantMongoWriter(jobOptions.getDbCollectionsVariantsName(),
                mongoOperations,
                variantToMongoDbObjectConverter(jobOptions));
    }

    @Bean
    @StepScope
    public VariantToMongoDbObjectConverter variantToMongoDbObjectConverter(JobOptions jobOptions) throws Exception {
        return new VariantToMongoDbObjectConverter(
                jobOptions.getVariantOptions().getBoolean(VariantStorageManager.INCLUDE_STATS),
                jobOptions.getVariantOptions().getBoolean(VariantStorageManager.CALCULATE_STATS),
                jobOptions.getVariantOptions().getBoolean(VariantStorageManager.INCLUDE_SAMPLES),
                (VariantStorageManager.IncludeSrc) jobOptions.getVariantOptions()
                        .get(VariantStorageManager.INCLUDE_SRC));
    }

}
