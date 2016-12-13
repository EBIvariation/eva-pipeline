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

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import uk.ac.ebi.eva.utils.MongoConnection;
import uk.ac.ebi.eva.utils.MongoDBHelper;

import javax.annotation.PostConstruct;

import java.net.UnknownHostException;

/**
 * Mongo variant reader using an ItemReader cursor based. This is speeding up the reading of the variant in big
 * collections. The {@link org.springframework.batch.item.data.MongoItemReader} is using pagination and it is slow with
 * large collections
 */
public class NonAnnotatedVariantsMongoReader extends MongoDbCursorItemReader {

    public NonAnnotatedVariantsMongoReader(String databaseName, String collectionsVariantsName,
                                           MongoConnection connection) throws UnknownHostException {
        super();

        setMongo(MongoDBHelper.getMongoClient(connection));

        setDatabaseName(databaseName);
        setCollectionName(collectionsVariantsName);

        DBObject refDbObject = BasicDBObjectBuilder.start().add("annot", new BasicDBObject("$exists", false)).get();
        setRefDbObject(refDbObject);

        String[] fields = {"chr", "start", "end", "ref", "alt"};
        setFields(fields);
    }

    @PostConstruct
    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
    }
}
