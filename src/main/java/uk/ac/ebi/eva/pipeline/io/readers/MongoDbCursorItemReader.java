/*
 * Copyright 2012 the original author or authors.
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
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

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.Map;


/**
 * Mongo item reader that is based on cursors, instead of the pagination used in the default Spring Data MongoDB
 * reader.
 * <p>
 * Its implementation is based on the one available in
 * <a href="https://github.com/acogoluegnes/Spring-Batch-MongoDB/blob/master/src/main/java/com/zenika/batch/item/database/mongo/MongoDbCursorItemReader.java</a>
 * but replaces the direct access to Mongo with a {@link MongoOperations}, following the Spring Data MongoDB model.
 */
public class MongoDbCursorItemReader extends AbstractItemCountingItemStreamItemReader<DBObject>
        implements InitializingBean {

    private MongoOperations template;
    private String collectionName;

    private DBObject query;
    private DBObject sort;
    private String[] fields;

    private DBCursor cursor;

    public MongoDbCursorItemReader() {
        super();
        setName(ClassUtils.getShortName(MongoDbCursorItemReader.class));
    }

    /**
     * Used to perform operations against the MongoDB instance. Also handles the
     * mapping of documents to objects.
     *
     * @param template The MongoOperations instance to use
     * @see MongoOperations
     */
    public void setTemplate(MongoOperations template) {
        this.template = template;
    }

    /**
     * A DBObject representing the MongoDB query.
     *
     * @param query Mongo query to run
     */
    public void setQuery(DBObject query) {
        if (query == null) {
            this.query = new BasicDBObject();
        } else {
            this.query = query;
        }
    }

    /**
     * List of fields to be returned from the matching documents by MongoDB.
     *
     * @param fields List of fields to return.
     */
    public void setFields(String... fields) {
        this.fields = fields;
    }

    /**
     * {@link Map} of property names/
     * {@link org.springframework.data.domain.Sort.Direction} values to sort the
     * input by.
     *
     * @param sorts Map of properties and direction to sort each.
     */
    public void setSort(Map<String, Sort.Direction> sorts) {
        this.sort = convertToSort(sorts);
    }

    /**
     * Name of the Mongo collection to be queried.
     *
     * @param collection Name of the collection
     */
    public void setCollection(String collection) {
        this.collectionName = collection;
    }

    @Override
    protected void doOpen() throws Exception {
        DBCollection collection = template.getCollection(collectionName);
        cursor = collection.find(query, createDbObjectKeys());
        if (sort != null) {
            cursor = cursor.sort(sort);
        }
    }

    @Override
    protected DBObject doRead() throws Exception {
        if (!cursor.hasNext()) {
            return null;
        } else {
            return cursor.next();
        }
    }

    @Override
    protected void doClose() throws Exception {
        cursor.close();
    }

    /**
     * Checks mandatory properties
     *
     * @see InitializingBean#afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(template, "An implementation of MongoOperations is required.");
        Assert.notNull(collectionName, "collectionName must be set");
        Assert.notNull(query, "A query is required.");
    }

    private DBObject createDbObjectKeys() {
        if (fields == null) {
            return new BasicDBObject();
        } else {
            BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
            for (String field : fields) {
                builder.add(field, 1);
            }
            return builder.get();
        }
    }

    private DBObject convertToSort(Map<String, Sort.Direction> sorts) {
        BasicDBObject sort = new BasicDBObject();

        for (Map.Entry<String, Sort.Direction> currSort : sorts.entrySet()) {
            sort.append(currSort.getKey(), currSort.getValue());
        }

        return sort;
    }
}
