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

package embl.ebi.variation.eva.pipeline.annotation.load;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import embl.ebi.variation.eva.pipeline.MongoDBHelper;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantAnnotationConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.MongoOperations;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Diego Poggioli
 *
 * Write a list of {@link VariantAnnotation} into MongoDB
 *
 * A new annotation is added in the existing document.
 * In case of two annotations (or more) in the same variant the other annotations are appended:
 *
 * 20_63963_G/A	20:63963	A	ENSG00000178591	ENST00000382410	Transcript	upstream_gene_variant	-	-	-	-	-	-	DISTANCE=4388;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=protein_coding;CANONICAL=YES;CCDS=CCDS12989.2;ENSP=ENSP00000371847;SWISSPROT=DB125_HUMAN;TREMBL=B2R4E8_HUMAN;UNIPARC=UPI00001A36DE
 * 20_63963_G/A	20:63963	A	ENSG00000178591	ENST00000608838	Transcript	upstream_gene_variant	-	-	-	-	-	-	DISTANCE=3928;STRAND=1;SYMBOL=DEFB125;SYMBOL_SOURCE=HGNC;HGNC_ID=18105;BIOTYPE=processed_transcript
 *
 * Will be:
 *
 * "annot" : {
 *  "ct" : [
 *      { "gn" : "DEFB125", "ensg" : "ENSG00000178591", "enst" : "ENST00000382410", "codon" : "-", "strand" : "+", "bt" : "protein_coding", "aaChange" : "-", "so" : [ 1631 ] },
 *      { "gn" : "DEFB125", "ensg" : "ENSG00000178591", "enst" : "ENST00000608838", "codon" : "-", "strand" : "+", "bt" : "processed_transcript", "aaChange" : "-",
 *  "so" : [ 1631 ] } ],
 *  "xrefs" : [
 *      { "id" : "DEFB125", "src" : "HGNC" },
 *      { "id" : "ENST00000382410", "src" : "ensemblTranscript" },
 *      { "id" : "ENST00000608838", "src" : "ensemblTranscript" },
 *      { "id" : "ENSG00000178591", "src" : "ensemblGene"
 */
public class VariantAnnotationMongoItemWriter extends MongoItemWriter<VariantAnnotation> {
    private static final Logger logger = LoggerFactory.getLogger(VariantAnnotationMongoItemWriter.class);

    private MongoOperations mongoOperations;
    private String collection;
    private DBObjectToVariantAnnotationConverter converter;

    public VariantAnnotationMongoItemWriter(MongoOperations mongoOperations) {
        this.mongoOperations = mongoOperations;
        converter = new DBObjectToVariantAnnotationConverter();
    }

    @Override
    public void setCollection(String collection) {
        super.setCollection(collection);
        this.collection = collection;
    }

    @Override
    protected void doWrite(List<? extends VariantAnnotation> variantAnnotations) {

        for (VariantAnnotation variantAnnotation : variantAnnotations) {
            logger.debug("Writing into mongo {}", variantAnnotation.getStart());

            String storageId = MongoDBHelper.buildStorageId(
                    variantAnnotation.getChromosome(),
                    variantAnnotation.getStart(),
                    variantAnnotation.getReferenceAllele(),
                    variantAnnotation.getAlternativeAllele());

            BasicDBObject find = new BasicDBObject("_id", storageId);

            //find existing variant with same Id, and bring only the annotation
            DBObject existingVariantStorage = mongoOperations.getCollection(collection)
                    .findOne(find, new BasicDBObject("annot", 1));

            //update annotation in existing variant
            if (existingVariantStorage != null){
                if(existingVariantStorage.containsField("annot")){

                    //update ConsequenceTypes
                    VariantAnnotation existingAnnotation = converter.convertToDataModelType((DBObject) existingVariantStorage.get("annot"));

                    if(variantAnnotation.getConsequenceTypes() != null){
                        existingAnnotation.getConsequenceTypes().addAll(variantAnnotation.getConsequenceTypes());
                    }

                    //update Hgvs
                    if(variantAnnotation.getHgvs() != null){
                        if(null == existingAnnotation.getHgvs()){
                            existingAnnotation.setHgvs(new ArrayList<>());
                        }
                        existingAnnotation.getHgvs().addAll(variantAnnotation.getHgvs());
                    }

                    variantAnnotation = existingAnnotation;
                }
            }

            DBObject storageVariantAnnotation = converter.convertToStorageType(variantAnnotation);

            BasicDBObject update = new BasicDBObject("$set", new BasicDBObject("annot", storageVariantAnnotation));
            mongoOperations.getCollection(collection).update(find, update);

        }
    }

}
