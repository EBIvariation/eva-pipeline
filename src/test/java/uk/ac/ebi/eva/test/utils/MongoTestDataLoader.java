package uk.ac.ebi.eva.test.utils;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import org.bson.BsonBinaryReader;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class MongoTestDataLoader {

    private static final int BATCH_SIZE = 1_000;

    private final MongoTemplate mongoTemplate;
    private final ResourceLoader resourceLoader;
    private final ObjectMapper objectMapper;

    public MongoTestDataLoader(MongoTemplate mongoTemplate,
                               ResourceLoader resourceLoader) {
        this(mongoTemplate, resourceLoader, new ObjectMapper());
    }

    public MongoTestDataLoader(MongoTemplate mongoTemplate,
                               ResourceLoader resourceLoader,
                               ObjectMapper objectMapper) {
        this.mongoTemplate = mongoTemplate;
        this.resourceLoader = resourceLoader;
        this.objectMapper = objectMapper;
    }


    public void load(String resourcePath, String collectionName) {
        load(resourcePath, collectionName, false);
    }

    public void load(String resourcePath, String collectionName, boolean dropCollection) {
        try {
            Resource resource = resourceLoader.getResource("classpath:" + resourcePath);

            try (InputStream is = resource.getInputStream()) {
                JsonNode root = objectMapper.readTree(is);

                JsonNode dataNode = root.has(collectionName) ? root.get(collectionName) : root;

                List<Document> documents = new ArrayList<>();

                if (dataNode.isArray()) {
                    for (JsonNode node : dataNode) {
                        documents.add(Document.parse(node.toString()));
                    }
                } else if (dataNode.isObject()) {
                    documents.add(Document.parse(dataNode.toString()));
                } else {
                    throw new IllegalArgumentException(
                            "Expected array or object for collection: " + collectionName);
                }

                // delete existing documents and insert the given documents
                if (dropCollection) {
                    mongoTemplate.getCollection(collectionName).deleteMany(new Document());
                }
                if (!documents.isEmpty()) {
                    mongoTemplate.getCollection(collectionName).insertMany(documents);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to load test data", e);
        }
    }

    public void restoreDumpFromFolder(String dumpFolder) throws IOException {
        Resource resource = resourceLoader.getResource("classpath:" + dumpFolder);

        try (Stream<Path> paths = Files.list(resource.getFile().toPath())) {
            paths.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".bson"))
                    .sorted()
                    .forEach(path -> {
                        try {
                            restoreDumpFromFile(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    public void restoreDumpFromFile(Path bsonFile) throws IOException {
        String collectionName = bsonFile.getFileName().toString().replaceFirst("\\.bson$", "");
        MongoCollection<Document> collection = mongoTemplate.getCollection(collectionName);

        byte[] bytes = Files.readAllBytes(bsonFile);
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        DocumentCodec codec = new DocumentCodec();
        DecoderContext decoderContext = DecoderContext.builder().build();
        List<Document> batch = new ArrayList<>(BATCH_SIZE);

        while (buffer.hasRemaining()) {
            int start = buffer.position();

            if (buffer.remaining() < Integer.BYTES) {
                throw new IOException("Truncated BSON file: " + bsonFile);
            }

            int documentSize = buffer.getInt();
            if (documentSize < 5) {
                throw new IOException("Invalid BSON document size " + documentSize + " in " + bsonFile);
            }

            buffer.position(start);
            if (buffer.remaining() < documentSize) {
                throw new IOException("Truncated BSON document in " + bsonFile);
            }

            byte[] documentBytes = new byte[documentSize];
            buffer.get(documentBytes);

            try (BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(documentBytes))) {
                batch.add(codec.decode(reader, decoderContext));
            }

            if (batch.size() >= BATCH_SIZE) {
                collection.insertMany(batch);
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            collection.insertMany(batch);
        }
    }
}