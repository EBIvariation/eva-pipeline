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

package uk.ac.ebi.eva.pipeline.io.writers;

import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.BasicUpdate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.Assert;
import uk.ac.ebi.eva.commons.mongodb.entities.AnnotationMongo;
import uk.ac.ebi.eva.commons.mongodb.entities.projections.SimplifiedAnnotation;
import uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.ConsequenceTypeMongo;
import uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.XrefMongo;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static uk.ac.ebi.eva.commons.mongodb.entities.AnnotationMongo.CONSEQUENCE_TYPE_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.AnnotationMongo.END_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.AnnotationMongo.VEP_CACHE_VERSION_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.AnnotationMongo.VEP_VERSION_FIELD;
import static uk.ac.ebi.eva.commons.mongodb.entities.AnnotationMongo.XREFS_FIELD;

/**
 * Write a list of {@link AnnotationMongo} into MongoDB
 * <p>
 * A new annotation is added in the existing document.
 * In case of two annotations (or more) in the same variant the other annotations are appended:
 * <p>
 * 20_63963_G/A    20:63963   A  ENSG00000178591    ENST00000382410    Transcript upstream_gene_variant  -  -  -  -  -  -  DISTANCE=4388;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=protein_coding;CANONICAL=YES;CCDS=CCDS12989.2;ENSP=ENSP00000371847;SWISSPROT=DB125_HUMAN;TREMBL=B2R4E8_HUMAN;UNIPARC=UPI00001A36DE
 * 20_63963_G/A    20:63963   A  ENSG00000178591    ENST00000608838    Transcript upstream_gene_variant  -  -  -  -  -  -  DISTANCE=3928;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript
 * <p>
 * Will be:
 * <p>
 * "annot" : {
 * "ct" : [
 * { "gn" : "DEFB125", "ensg" : "ENSG00000178591", "enst" : "ENST00000382410", "codon" : "-", "strand" : "+", "bt" : "protein_coding", "aaChange" : "-", "so" : [ 1631 ] },
 * { "gn" : "DEFB125", "ensg" : "ENSG00000178591", "enst" : "ENST00000608838", "codon" : "-", "strand" : "+", "bt" : "processed_transcript", "aaChange" : "-",
 * "so" : [ 1631 ] } ],
 * "xrefs" : [
 * { "id" : "DEFB125", "src" : "HGNC" },
 * { "id" : "ENST00000382410", "src" : "ensemblTranscript" },
 * { "id" : "ENST00000608838", "src" : "ensemblTranscript" },
 * { "id" : "ENSG00000178591", "src" : "ensemblGene"
 */
public class AnnotationMongoWriter implements ItemWriter<List<AnnotationMongo>> {

    private static final String ANNOTATION_XREF_ID_FIELD = XREFS_FIELD + "." + XrefMongo.XREF_ID_FIELD;

    private static final String ANNOTATION_CT_SO_FIELD = CONSEQUENCE_TYPE_FIELD + "."
            + ConsequenceTypeMongo.SO_ACCESSION_FIELD;

    public static final String EACH = "$each";
    public static final String ADD_TO_SET = "$addToSet";
    public static final String SET_ON_INSERT = "$setOnInsert";

    private final MongoOperations mongoOperations;

    private final String collection;

    public AnnotationMongoWriter(MongoOperations mongoOperations, String collection) {
        Assert.notNull(mongoOperations);
        Assert.hasText(collection);
        this.mongoOperations = mongoOperations;
        this.collection = collection;

        createIndexes();
    }

    @Override
    public void write(List<? extends List<AnnotationMongo>> annotations) throws Exception {
        for (List<AnnotationMongo> annotationList : annotations) {
            BulkOperations bulk = mongoOperations.bulkOps(BulkOperations.BulkMode.UNORDERED, collection);
            prepareBulk(annotationList, bulk);
            bulk.execute();
        }
    }

    private void prepareBulk(List<? extends AnnotationMongo> annotations, BulkOperations bulk) {
        Map<String, AnnotationMongo> annotationsByStorageId = groupAnnotationById(annotations);
        for (AnnotationMongo annotation : annotationsByStorageId.values()) {
            writeAnnotationInMongoDb(bulk, annotation);
        }
    }

    private Map<String, AnnotationMongo> groupAnnotationById(List<? extends AnnotationMongo> annotations) {
        Map<String, AnnotationMongo> groupedAnnotations = new HashMap<>();
        for (AnnotationMongo annotation : annotations) {
            String id = annotation.getId();
            groupedAnnotations.computeIfPresent(id, (key, oldVar) -> oldVar.concatenate(annotation));
            groupedAnnotations.putIfAbsent(id, annotation);
        }
        return groupedAnnotations;
    }

    private void writeAnnotationInMongoDb(BulkOperations bulk, AnnotationMongo annotation) {
        Query upsertQuery = new BasicQuery(convertToMongo(new SimplifiedAnnotation(annotation)));
        Update update = buildUpdateQuery(annotation);
        bulk.upsert(upsertQuery, update);
    }

    private BasicUpdate buildUpdateQuery(AnnotationMongo annotation) {
        final Document setOnInsertValue = new Document();
        setOnInsertValue.append(END_FIELD, annotation.getEnd());
        setOnInsertValue.append(VEP_VERSION_FIELD, annotation.getVepVersion());
        setOnInsertValue.append(VEP_CACHE_VERSION_FIELD, annotation.getVepCacheVersion());

        final Document addToSetValue = new Document();
        addToSetValue.append(CONSEQUENCE_TYPE_FIELD, buildInsertConsequenceTypeQuery(annotation));
        addToSetValue.append(XREFS_FIELD, buildInsertXrefsQuery(annotation));

        final Document updateDoc = new Document(ADD_TO_SET, addToSetValue);
        updateDoc.append(SET_ON_INSERT, setOnInsertValue);
        return new BasicUpdate(updateDoc);
    }

    private Document buildInsertXrefsQuery(AnnotationMongo annotation) {
        return new Document(EACH, convertToMongo(annotation.getXrefs()));
    }

    private Document buildInsertConsequenceTypeQuery(AnnotationMongo annotation) {
        return new Document(EACH, convertToMongo(annotation.getConsequenceTypes()));
    }

    private Document convertToMongo(SimplifiedAnnotation simplifiedAnnotation) {
        return (Document) mongoOperations.getConverter().convertToMongoType(simplifiedAnnotation);
    }

    private List<Document> convertToMongo(Collection<?> object) {
        return (List<Document>) mongoOperations.getConverter().convertToMongoType(object);
    }

    private void createIndexes() {
        mongoOperations.getCollection(collection).createIndex(
                new Document(ANNOTATION_XREF_ID_FIELD, 1), new IndexOptions().background(true));
        mongoOperations.getCollection(collection).createIndex(
                new Document(ANNOTATION_CT_SO_FIELD, 1), new IndexOptions().background(true));
    }
}
