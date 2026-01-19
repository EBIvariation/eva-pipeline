package uk.ac.ebi.eva.test.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MongoCollectionNameConfiguration {

    @Bean(name = "mongoCollectionsVariants")
    public String mongoCollectionsVariants() {
        return "variants";
    }

    @Bean(name = "mongoCollectionsFiles")
    public String mongoCollectionsFiles() {
        return "files";
    }
}
