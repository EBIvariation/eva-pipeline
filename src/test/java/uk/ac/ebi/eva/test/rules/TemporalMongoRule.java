package uk.ac.ebi.eva.test.rules;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class TemporalMongoRule extends ExternalResource {

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

    public String createTemporalDatabase() {
        return getDatabase().getName();
    }

    public DB createTemporalDatabase(String dbName) {
        return getDatabase(dbName);
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
        return getDatabase(databaseName).getCollection(collection);
    }

//    public DBCollection getCollection(String collection) {
//        return getDatabase().getCollection(collection);
//    }
//
//    public DBCollection getCollection() {
//        return getCollection(UUID.randomUUID().toString());
//    }

    /**
     * Returns a new temporal database
     *
     * @return
     */
    private DB getDatabase() {
        return getDatabase(UUID.randomUUID().toString());
    }

    /**
     * Returns a temporal database with {@param database} name
     *
     * @param databaseName
     * @return
     */
    public DB getDatabase(String databaseName) {
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

}
