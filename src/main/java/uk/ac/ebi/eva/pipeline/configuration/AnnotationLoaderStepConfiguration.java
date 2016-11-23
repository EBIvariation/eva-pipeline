package uk.ac.ebi.eva.pipeline.configuration;

import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.data.mongodb.core.MongoOperations;
import uk.ac.ebi.eva.pipeline.Application;
import uk.ac.ebi.eva.pipeline.io.writers.VepAnnotationMongoWriter;
import uk.ac.ebi.eva.utils.MongoDBHelper;

@Configuration
public class AnnotationLoaderStepConfiguration {

    @Autowired
    private JobOptions jobOptions;

    @Bean
    @Scope("prototype")
    @Profile(Application.VARIANT_ANNOTATION_MONGO_PROFILE)
    public ItemWriter<VariantAnnotation> variantAnnotationItemWriter() {
        MongoOperations mongoOperations = MongoDBHelper.getMongoOperationsFromPipelineOptions(jobOptions.getPipelineOptions());
        String collections = jobOptions.getPipelineOptions().getString(JobParametersNames.DB_COLLECTIONS_VARIANTS_NAME);
        return new VepAnnotationMongoWriter(mongoOperations, collections);
    }

}
