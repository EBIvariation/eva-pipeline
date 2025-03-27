package uk.ac.ebi.eva.test.rules;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TemporaryMongoRule extends ExternalResource {

    private static final Logger logger = LoggerFactory.getLogger(TemporaryMongoRule.class);

    private final Set<String> databaseNames;
    private MongoClient mongoClient;
    private final String mongoHost;
    private final int mongoPort;

    public TemporaryMongoRule(String mongoHost, int mongoPort) {
        databaseNames = new HashSet<>();
        this.mongoHost = mongoHost;
        this.mongoPort = mongoPort;
    }

    public TemporaryMongoRule(String mongoUri) {
        databaseNames = new HashSet<>();
        Pattern p = Pattern.compile("mongodb://(.*?):([0-9]+).*");
        Matcher m = p.matcher(mongoUri);
        if (m.find()) {
            this.mongoHost = m.group(1);
            this.mongoPort = Integer.parseInt(m.group(2));
        } else {
            throw new IllegalArgumentException("Invalid mongo URI: " + mongoUri);
        }
    }

    @Override
    protected void after() {
        cleanDBs();
        mongoClient.close();
    }

    @Override
    protected void before() throws Throwable {
        logger.info("Creating MongoClient with MongoHost: " + this.mongoHost);
        mongoClient = new MongoClient(this.mongoHost, this.mongoPort);
    }

    public String getRandomTemporaryDatabaseName() {
        return getTemporaryDatabase().getName();
    }

    /**
     * Returns a DBObject obtained by parsing a given string
     *
     * @param variant string in JSON format
     * @return DBObject
     */
    public static Document constructDbObject(String variant) {
        return Document.parse(variant);
    }

    public MongoCollection<Document> getCollection(String databaseName, String collection) {
        return getTemporaryDatabase(databaseName).getCollection(collection);
    }

    /**
     * Returns a new temporary database
     *
     * @return
     */
    private MongoDatabase getTemporaryDatabase() {
        return getTemporaryDatabase(UUID.randomUUID().toString());
    }

    /**
     * Returns a temporary database with {@param database} name
     *
     * @param databaseName
     * @return
     */
    public MongoDatabase getTemporaryDatabase(String databaseName) {
        return mongoClient.getDatabase(databaseName);
    }


    private void cleanDBs() {
        for (String databaseName : databaseNames) {
            DB database = mongoClient.getDB(databaseName);
            database.dropDatabase();
        }
        databaseNames.clear();
    }

    public void insert(String databaseName, String collectionName, String jsonString) {
        getCollection(databaseName, collectionName).insertOne(constructDbObject(jsonString));
    }

    public String restoreDumpInTemporaryDatabase(URL dumpLocation) throws IOException, InterruptedException {
        String databaseName = getRandomTemporaryDatabaseName();
        restoreDump(dumpLocation, databaseName);
        return databaseName;
    }

    public void restoreDump(URL dumpLocation, String databaseName) throws IOException, InterruptedException {
        assert (dumpLocation != null);
        assert (databaseName != null && !databaseName.isEmpty());
        String file = dumpLocation.getFile();
        assert (file != null && !file.isEmpty());
        getTemporaryDatabase(databaseName);

        logger.info("restoring DB from " + file + " into database " + databaseName + "with host:port " + this.mongoHost + ":" + this.mongoPort);

        Process exec = Runtime.getRuntime().exec(String.format("mongorestore -h %s:%s -d %s %s", this.mongoHost, this.mongoPort, databaseName, file));
        exec.waitFor();
        String line;
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(exec.getInputStream()));
        while ((line = bufferedReader.readLine()) != null) {
            logger.info("mongorestore output:" + line);
        }
        bufferedReader.close();
        bufferedReader = new BufferedReader(new InputStreamReader(exec.getErrorStream()));
        while ((line = bufferedReader.readLine()) != null) {
            logger.info("mongorestore errorOutput:" + line);
        }
        bufferedReader.close();

        logger.info("mongorestore exit value: " + exec.exitValue());
    }

    public String createDBAndInsertDocuments(String collectionName, Collection<String> documents) {
        String databaseName = getRandomTemporaryDatabaseName();
        insertDocuments(databaseName, collectionName, documents);
        return databaseName;
    }

    public void insertDocuments(String databaseName, String collectionName, Collection<String> documents) {
        for (String document : documents) {
            insert(databaseName, collectionName, document);
        }
    }
}
