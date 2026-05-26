package uk.ac.ebi.eva.test.utils;

import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public abstract class MongoTestContainerHelper {

    private static final String MONGO_IMAGE = "mongo:6.0";

    @Container
    @ServiceConnection
    public static MongoDBContainer mongo = new MongoDBContainer(MONGO_IMAGE);

    @DynamicPropertySource
    public static void registerMongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", mongo::getConnectionString);
    }
}