/*
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
import com.mongodb.DBObject;
import org.springframework.data.mongodb.core.MongoOperations;

import uk.ac.ebi.eva.commons.models.converters.data.VariantSourceEntryToDBObjectConverter;
import uk.ac.ebi.eva.commons.models.converters.data.VariantToDBObjectConverter;

import javax.annotation.PostConstruct;

/**
 * Mongo variant reader using an ItemReader cursor based. This is speeding up
 * the reading of the variant in big collections. The
 * {@link org.springframework.batch.item.data.MongoItemReader} is using
 * pagination and it is slow with large collections
 */
public class NonAnnotatedVariantsMongoReader extends MongoDbCursorItemReader {

    private static final String STUDY_KEY = VariantToDBObjectConverter.FILES_FIELD + "."
            + VariantSourceEntryToDBObjectConverter.STUDYID_FIELD;

    /**
     * @param studyId nullable. If null, the query brings all non-annotated variants in the collection.
     *  If not null, bring only non-annotated variants from that study.
     */
    public NonAnnotatedVariantsMongoReader(MongoOperations template, String collectionsVariantsName, String studyId) {
        setTemplate(template);
        setCollection(collectionsVariantsName);

        BasicDBObjectBuilder queryBuilder = BasicDBObjectBuilder.start();
        if (!studyId.isEmpty()) {
            queryBuilder.add(STUDY_KEY, studyId);
        }
        DBObject query = queryBuilder.add("annot.ct.so", new BasicDBObject("$exists", false)).get();
        setQuery(query);

        String[] fields = { "chr", "start", "end", "ref", "alt" };
        setFields(fields);
    }

    @PostConstruct
    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
    }
}
