package uk.ac.ebi.eva.pipeline.configuration;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

@Configuration
public class MongoCollectionNameConfiguration {

    @Bean
    public String mongoCollectionsAnnotationMetadata(
            @Value("${db.collections.annotation-metadata.name:#{null}}") String collectionAnnotationMetadata) {
        Assert.notNull(collectionAnnotationMetadata);
        return collectionAnnotationMetadata;
    }

    @Bean
    public String mongoCollectionsAnnotations(
            @Value("${db.collections.annotations.name:#{null}}") String collectionAnnotations) {
        Assert.notNull(collectionAnnotations);
        return collectionAnnotations;
    }

    @Bean
    public String mongoCollectionsVariants(
            @Value("${db.collections.variants.name:#{null}}") String collectionVariants) {
        Assert.notNull(collectionVariants);
        return collectionVariants;
    }

    @Bean
    public String mongoCollectionsFiles(@Value("${db.collections.files.name:#{null}}") String collectionFiles) {
        Assert.notNull(collectionFiles);
        return collectionFiles;
    }
}
