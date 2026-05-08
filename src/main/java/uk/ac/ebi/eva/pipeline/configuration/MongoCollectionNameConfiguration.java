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
        Assert.notNull(collectionAnnotationMetadata, "Collection name for annotation metadata must be provided");
        return collectionAnnotationMetadata;
    }

    @Bean
    public String mongoCollectionsAnnotations(
            @Value("${db.collections.annotations.name:#{null}}") String collectionAnnotations) {
        Assert.notNull(collectionAnnotations, "Collection name for annotations must be provided");
        return collectionAnnotations;
    }

    @Bean
    public String mongoCollectionsVariants(
            @Value("${db.collections.variants.name:#{null}}") String collectionVariants) {
        Assert.notNull(collectionVariants, "Collection name for variants must be provided");
        return collectionVariants;
    }

    @Bean
    public String mongoCollectionsFiles(@Value("${db.collections.files.name:#{null}}") String collectionFiles) {
        Assert.notNull(collectionFiles, "Collection name for files must be provided");
        return collectionFiles;
    }
}
