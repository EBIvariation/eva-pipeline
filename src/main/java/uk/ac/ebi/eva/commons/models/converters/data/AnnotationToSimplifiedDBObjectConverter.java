/*
 * Copyright 2014-2016 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.commons.models.converters.data;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.springframework.core.convert.converter.Converter;
import uk.ac.ebi.eva.commons.models.mongo.documents.Annotation;

/**
 * Class to convert a Annotation document to a DBObject that contains a simplified version of the document.
 * This is done to aggregate the different consequences of the same Annotation in the database itself. It is not a
 * complete converter of the Annotation object.
 */
public class AnnotationToSimplifiedDBObjectConverter implements Converter<Annotation, DBObject> {

    @Override
    public DBObject convert(Annotation source) {
        DBObject dbObject = new BasicDBObject();
        dbObject.put("_id",source.getId());
        dbObject.put(Annotation.CHROMOSOME_FIELD,source.getChromosome());
        dbObject.put(Annotation.START_FIELD,source.getStart());
        dbObject.put(Annotation.END_FIELD,source.getEnd());
        dbObject.put(Annotation.VEP_VERSION_FIELD,source.getVepVersion());
        dbObject.put(Annotation.VEP_CACHE_VERSION_FIELD,source.getVepCacheVersion());
        return dbObject;
    }

}
