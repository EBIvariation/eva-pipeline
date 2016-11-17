package uk.ac.ebi.eva.test.rules;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class TemporalMongoRule extends ExternalResource {

    private static final Logger logger = LoggerFactory.getLogger(TemporalMongoRule.class);

    private final Set<String> databaseNames;
    private Description description;
    private MongoClient mongoClient;

    public TemporalMongoRule() {
        databaseNames = new HashSet<>();
    }

    @Override
    public Statement apply(Statement base, Description description) {
        this.description = description;
        return super.apply(base, description);
    }

    @Override
    protected void after() {
        cleanDBs();
        mongoClient.close();
    }

    @Override
    protected void before() throws Throwable {
        mongoClient = new MongoClient();
    }

    public String getRandomTemporalDatabaseName() {
        return getTemporalDatabase().getName();
    }

    /**
     * Returns a DBObject obtained by parsing a given string
     *
     * @param variant string in JSON format
     * @return DBObject
     */
    public static DBObject constructDbo(String variant) {
        return (DBObject) JSON.parse(variant);
    }

    public DBCollection getCollection(String databaseName, String collection) {
        return getTemporalDatabase(databaseName).getCollection(collection);
    }

    /**
     * Returns a new temporal database
     *
     * @return
     */
    private DB getTemporalDatabase() {
        return getTemporalDatabase(UUID.randomUUID().toString());
    }

    /**
     * Returns a temporal database with {@param database} name
     *
     * @param databaseName
     * @return
     */
    public DB getTemporalDatabase(String databaseName) {
        databaseNames.add(databaseName);
        return mongoClient.getDB(databaseName);
    }


    private void cleanDBs() {
        for (String databaseName : databaseNames) {
            DB database = mongoClient.getDB(databaseName);
            database.dropDatabase();
        }
        databaseNames.clear();
    }

    public void insert(String databaseName, String collectionName, String jsonString) {
        getCollection(databaseName, collectionName).insert(constructDbo(jsonString));
    }

    public void importDump(URL dumpLocation, String databaseName) throws IOException, InterruptedException {
        assert (dumpLocation != null);
        assert (databaseName != null && !databaseName.isEmpty());
        String file = dumpLocation.getFile();
        assert (file != null && !file.isEmpty());
        getTemporalDatabase(databaseName);

        logger.info("restoring DB from " + file + " into database " + databaseName);

        Process exec = Runtime.getRuntime().exec(String.format("mongorestore -d %s %s", databaseName, file));
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
}
