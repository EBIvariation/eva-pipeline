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
package embl.ebi.variation.eva.pipeline.steps.readers;

import com.mongodb.DBObject;
import embl.ebi.variation.eva.pipeline.MongoDBHelper;
import org.opencb.datastore.core.ObjectMap;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.data.domain.Sort;

import java.util.HashMap;
import java.util.Map;

/**
 * Readers that extract from MongoDB all the documents without annotations
 * Fields extracted are: chr, start, end, ref, alt, type
 *
 */

public class VariantReader extends MongoItemReader<DBObject> {

    public VariantReader(ObjectMap pipelineOptions){
        super();

        setCollection(pipelineOptions.getString("db.collections.variants.name"));

        setQuery("{ annot : { $exists : false } }");
        setFields("{ chr : 1, start : 1, end : 1, ref : 1, alt : 1, type : 1}");
        setTargetType(DBObject.class);
        setTemplate(MongoDBHelper.getMongoOperationsFromPipelineOptions(pipelineOptions));

        Map<String, Sort.Direction> coordinatesSort = new HashMap<>();
        coordinatesSort.put("chr", Sort.Direction.ASC);
        coordinatesSort.put("start", Sort.Direction.ASC);
        setSort(coordinatesSort);
    }

}
