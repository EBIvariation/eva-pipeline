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
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.util.ClassUtils;

import uk.ac.ebi.eva.commons.models.mongo.entity.Annotation;
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument;
import uk.ac.ebi.eva.commons.models.mongo.entity.projections.SimplifiedVariant;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantAnnotation;
import uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.VariantSourceEntryMongo;
import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;

import javax.annotation.PostConstruct;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

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
        extends AbstractItemStreamItemReader<List<EnsemblVariant>> implements InitializingBean {

    private static final String STUDY_KEY = VariantDocument.FILES_FIELD + "." + VariantSourceEntryMongo.STUDYID_FIELD;

    private static final String FILE_KEY = VariantDocument.FILES_FIELD + "." + VariantSourceEntryMongo.FILEID_FIELD;

    private static final String LAST_READ_TIMESTAMP_KEY = "last_read_timestamp";

    private MongoDbCursorItemReader delegateReader;

    private MongoConverter converter;

    private Integer chunkSize;

    private ZonedDateTime lastRead;

    /**
     *
     * @param vepVersion Only bring variants whose annotation does not contain this VEP version.
     *                   This option is ignored if excludeAnnotated is false.
     * @param vepCacheVersion Only bring variants whose annotation does not contain this cache version.
     *                        This option is ignored if excludeAnnotated is false.
     * @param studyId Can be the empty string or null, meaning to bring all non-annotated variants in the collection.
     *                If the studyId string is not empty, bring only non-annotated variants from that study.
     * @param fileId  File identifier that it is checked inside a study. If the study identifier is not defined, the
     *                file is ignored. This is mainly due to performance reasons.
     *                Can be the empty string or null, meaning to bring all non-annotated variants in a study.
     *                If the studyId string is not empty, bring only non-annotated variants from that study and file.
     * @param excludeAnnotated If true, bring only non-annotated variants. If false, bring all variants (ignoring the
     *                         vepVersion and vepCacheVersion parameters)
     * @param chunkSize size of the list returned by the "read" method.
     */
    public VariantsMongoReader(MongoOperations mongoOperations, String collectionVariantsName, String vepVersion,
                               String vepCacheVersion, String studyId, String fileId, boolean excludeAnnotated,
                               Integer chunkSize) {
        setName(ClassUtils.getShortName(VariantsMongoReader.class));
        delegateReader = new MongoDbCursorItemReader();
        delegateReader.setTemplate(mongoOperations);
        delegateReader.setCollection(collectionVariantsName);
        
        // the query excludes processed variants automatically, so a new query has to start from the beginning
        delegateReader.setSaveState(false);

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

        this.converter = mongoOperations.getConverter();
        this.chunkSize = chunkSize;
        this.lastRead = ZonedDateTime.now();
    }

    @PostConstruct
    @Override
    public void afterPropertiesSet() throws Exception {
        delegateReader.afterPropertiesSet();
    }

    @Override
    public void open(ExecutionContext executionContext) {
        delegateReader.open(executionContext);
    }

    @Override
    public List<EnsemblVariant> read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        List<EnsemblVariant> variants = readBatch(chunkSize);
        if (variants.size() == 0) {
            return null;
        } else {
            return variants;
        }
    }

    private List<EnsemblVariant> readBatch(Integer chunkSize) throws Exception {
        List<EnsemblVariant> variants = new ArrayList<>();
        DBObject dbObject;
        while ((dbObject = delegateDoRead()) != null) {
            SimplifiedVariant variant = converter.read(SimplifiedVariant.class, dbObject);
            variants.add(buildVariantWrapper(variant));
            if (variants.size() == chunkSize) {
                break;
            }
        }
        return variants;
    }

    private DBObject delegateDoRead() throws Exception {
        lastRead = ZonedDateTime.now();
        return delegateReader.doRead();
    }

    private EnsemblVariant buildVariantWrapper(SimplifiedVariant variant) {
        return new EnsemblVariant(variant.getChromosome(),
                                  variant.getStart(),
                                  variant.getEnd(),
                                  variant.getReference(),
                                  variant.getAlternate());
    }

    @Override
    public void close() {
        delegateReader.close();
    }

    @Override
    public void update(ExecutionContext executionContext) {
        super.update(executionContext);

        // to debug EVA-781: mongo timeouts
        executionContext.put(LAST_READ_TIMESTAMP_KEY, lastRead.format(DateTimeFormatter.ISO_DATE_TIME));
    }
}
