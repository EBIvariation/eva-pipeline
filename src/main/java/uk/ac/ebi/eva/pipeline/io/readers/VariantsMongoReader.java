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
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantConverter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.util.ClassUtils;
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument;
import uk.ac.ebi.eva.commons.models.mongo.entity.projections.SimplifiedVariant;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo;
import uk.ac.ebi.eva.pipeline.model.VariantWrapper;

import javax.annotation.PostConstruct;

/**
 * Mongo variant reader using an ItemReader cursor based. This is speeding up
 * the reading of the variant in big collections. The
 * {@link org.springframework.batch.item.data.MongoItemReader} is using
 * pagination and it is slow with large collections
 */
public class VariantsMongoReader
        extends AbstractItemCountingItemStreamItemReader<VariantWrapper> implements InitializingBean {

    private MongoDbCursorItemReader delegateReader;

    private MongoConverter converter;

    private static final String STUDY_KEY = VariantDocument.FILES_FIELD + "." + VariantSourceEntryMongo.STUDYID_FIELD;

    /**
     * @param studyId Can be the empty string or null, meaning to bring all non-annotated variants in the collection.
     * If the studyId string is not empty, bring only non-annotated variants from that study.
     * @param excludeAnnotated bring only non-annotated variants.
     */
    public VariantsMongoReader(MongoOperations template, String collectionsVariantsName, String studyId,
                               boolean excludeAnnotated) {
        setName(ClassUtils.getShortName(VariantsMongoReader.class));
        delegateReader = new MongoDbCursorItemReader();
        delegateReader.setTemplate(template);
        delegateReader.setCollection(collectionsVariantsName);

        BasicDBObjectBuilder queryBuilder = BasicDBObjectBuilder.start();
        if (studyId != null && !studyId.isEmpty()) {
            queryBuilder.add(STUDY_KEY, studyId);
        }
        if (excludeAnnotated) {
            queryBuilder.add("annot.ct.so", new BasicDBObject("$exists", false));
        }
        delegateReader.setQuery(queryBuilder.get());

        String[] fields = {"chr", "start", "end", "ref", "alt"};
        delegateReader.setFields(fields);

        converter = template.getConverter();
    }

    @PostConstruct
    @Override
    public void afterPropertiesSet() throws Exception {
        delegateReader.afterPropertiesSet();
    }

    @Override
    protected void doOpen() throws Exception {
        delegateReader.doOpen();
    }

    @Override
    protected VariantWrapper doRead() throws Exception {
        DBObject dbObject = delegateReader.doRead();
        if (dbObject != null) {
            SimplifiedVariant variant = converter.read(SimplifiedVariant.class, dbObject);
            return buildVariantWrapper(variant);
        } else {
            return null;
        }
    }

    private VariantWrapper buildVariantWrapper(SimplifiedVariant variant) {
        return new VariantWrapper(variant.getChromosome(),
                                  variant.getStart(),
                                  variant.getEnd(),
                                  variant.getReference(),
                                  variant.getAlternate());
    }

    @Override
    protected void doClose() throws Exception {
        delegateReader.doClose();
    }

}
