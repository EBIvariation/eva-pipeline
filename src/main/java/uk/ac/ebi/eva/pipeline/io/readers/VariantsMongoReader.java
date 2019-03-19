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
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.util.ClassUtils;

import uk.ac.ebi.eva.commons.models.mongo.entity.Annotation;
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument;
import uk.ac.ebi.eva.commons.models.mongo.entity.projections.SimplifiedVariant;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantAnnotation;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo;
import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;

import javax.annotation.PostConstruct;

import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.ALTERNATE_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.CHROMOSOME_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.END_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.REFERENCE_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.START_FIELD;

/**
 * Mongo variant reader using an ItemReader cursor based. This is speeding up
 * the reading of the variant in big collections. The
 * {@link org.springframework.batch.item.data.MongoItemReader} is using
 * pagination and it is slow with large collections
 */
public class VariantsMongoReader
        extends AbstractItemCountingItemStreamItemReader<EnsemblVariant> implements InitializingBean {

    private MongoDbCursorItemReader delegateReader;

    private MongoConverter converter;

    private static final String STUDY_KEY = VariantDocument.FILES_FIELD + "." + VariantSourceEntryMongo.STUDYID_FIELD;

    private static final String FILE_KEY = VariantDocument.FILES_FIELD + "." + VariantSourceEntryMongo.FILEID_FIELD;

    /**
     * @param studyId Can be the empty string or null, meaning to bring all non-annotated variants in the collection.
     *                If the studyId string is not empty, bring only non-annotated variants from that study.
     * @param fileId  File identifier that it is checked inside a study. If the study identifier is not defined, the
     *                file is ignored. This is mainly due to performance reasons.
     *                Can be the empty string or null, meaning to bring all non-annotated variants in a study.
     *                If the studyId string is not empty, bring only non-annotated variants from that study and file.
     * @param excludeAnnotated bring only non-annotated variants.
     */
    public VariantsMongoReader(MongoOperations mongoOperations, String collectionVariantsName, String vepVersion,
                               String vepCacheVersion, String studyId, String fileId, boolean excludeAnnotated) {
        setName(ClassUtils.getShortName(VariantsMongoReader.class));
        delegateReader = new MongoDbCursorItemReader();
        delegateReader.setTemplate(mongoOperations);
        delegateReader.setCollection(collectionVariantsName);

        BasicDBObjectBuilder queryBuilder = BasicDBObjectBuilder.start();

        if (studyId != null && !studyId.isEmpty()) {
            queryBuilder.add(STUDY_KEY, studyId);

            if (fileId != null && !fileId.isEmpty()) {
                queryBuilder.add(FILE_KEY, fileId);
            }
        }

        if (excludeAnnotated) {
            BasicDBObject exists = new BasicDBObject("$exists", 1);
            BasicDBObject annotationSubdocument = new BasicDBObject(VariantAnnotation.SO_ACCESSION_FIELD, exists)
                    .append(Annotation.VEP_VERSION_FIELD, vepVersion)
                    .append(Annotation.VEP_CACHE_VERSION_FIELD, vepCacheVersion);
            BasicDBObject noElementMatchesOurVersion =
                    new BasicDBObject("$not", new BasicDBObject("$elemMatch", annotationSubdocument));
            queryBuilder.add(VariantDocument.ANNOTATION_FIELD, noElementMatchesOurVersion);
        }
        delegateReader.setQuery(queryBuilder.get());

        String[] fields = {CHROMOSOME_FIELD, START_FIELD, END_FIELD, REFERENCE_FIELD, ALTERNATE_FIELD};
        delegateReader.setFields(fields);

        converter = mongoOperations.getConverter();
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
    protected EnsemblVariant doRead() throws Exception {
        DBObject dbObject = delegateReader.doRead();
        if (dbObject != null) {
            SimplifiedVariant variant = converter.read(SimplifiedVariant.class, dbObject);
            return buildVariantWrapper(variant);
        } else {
            return null;
        }
    }

    private EnsemblVariant buildVariantWrapper(SimplifiedVariant variant) {
        return new EnsemblVariant(variant.getChromosome(),
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
