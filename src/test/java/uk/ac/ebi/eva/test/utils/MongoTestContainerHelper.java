package uk.ac.ebi.eva.test.utils;

import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public abstract class MongoTestContainerHelper {

    private static final DockerImageName MONGO_IMAGE =
            DockerImageName.parse("percona/percona-server-mongodb:8.0.38-16").asCompatibleSubstituteFor("mongo");

    @Container
    @ServiceConnection
    public static MongoDBContainer mongo = new MongoDBContainer(MONGO_IMAGE);

    @DynamicPropertySource
    public static void registerMongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", mongo::getConnectionString);
    }
}