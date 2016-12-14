package uk.ac.ebi.eva.pipeline.configuration;

import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.mongodb.core.MongoOperations;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.io.writers.VepAnnotationMongoWriter;
import uk.ac.ebi.eva.pipeline.parameters.JobOptions;
import uk.ac.ebi.eva.pipeline.parameters.JobParametersNames;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import java.net.UnknownHostException;

@Configuration
public class VariantAnnotationWriterConfiguration {

    @Bean
    @StepScope
    @Profile(Application.VARIANT_ANNOTATION_MONGO_PROFILE)
    public ItemWriter<VariantAnnotation> variantAnnotationItemWriter(JobOptions jobOptions) throws UnknownHostException {
        MongoOperations mongoOperations = MongoDBHelper.getMongoOperations(jobOptions
                .getDbName(), jobOptions.getMongoConnection());
        String collections = jobOptions.getPipelineOptions().getString(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME);
        return new VepAnnotationMongoWriter(mongoOperations, collections);
    }

}
