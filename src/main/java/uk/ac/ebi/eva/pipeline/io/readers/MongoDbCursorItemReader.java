/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.pipeline.io.readers;

import com.mongodb.*;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * @author Diego Poggioli
 *
 * @see <a href="https://github.com/acogoluegnes/Spring-Batch-MongoDB/blob/master/src/main/java/com/zenika/batch/item/database/mongo/MongoDbCursorItemReader.java</a>
 *
 * Mongo item reader cursor based
 */
public class MongoDbCursorItemReader extends AbstractItemCountingItemStreamItemReader<DBObject> implements InitializingBean {
    private Mongo mongo;

    private String databaseName;

    private String collectionName;

    private DBCursor cursor;

    private String [] fields;

    private DBObject refDbObject;

    public MongoDbCursorItemReader() {
        super();
        setName(ClassUtils.getShortName(MongoDbCursorItemReader.class));
    }

    @Override
    protected void doOpen() throws Exception {
        DBCollection collection = mongo.getDB(databaseName).getCollection(collectionName);
        cursor = collection.find(createDbObjectRef(), createDbObjectKeys());
    }

    @Override
    protected DBObject doRead() throws Exception {
        if(!cursor.hasNext()) {
            return null;
        } else {
            return cursor.next();
        }
    }

    @Override
    protected void doClose() throws Exception {
        cursor.close();
    }

    @Override
    protected void jumpToItem(int itemIndex) throws Exception {
        cursor = cursor.skip(itemIndex);
    }

    private DBObject createDbObjectKeys() {
        if(fields == null) {
            return new BasicDBObject();
        } else {
            BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
            for(String field : fields) {
                builder.add(field,1);
            }
            return builder.get();
        }
    }

    private DBObject createDbObjectRef() {
        if(refDbObject == null) {
            return new BasicDBObject();
        } else {
            return refDbObject;
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(mongo,"Mongo must be specified");
        Assert.notNull(databaseName,"Mongo AND database must be set");
        Assert.notNull(collectionName,"collectionName must be set");
    }

    public void setRefDbObject(DBObject refDbObject) {
        this.refDbObject = refDbObject;
    }

    public void setMongo(Mongo mongo) {
        this.mongo = mongo;
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

}