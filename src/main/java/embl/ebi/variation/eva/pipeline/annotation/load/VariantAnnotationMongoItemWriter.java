/*
 * Copyright 2015-2016 EMBL - European Bioinformatics Institute
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
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.commons.utils.CryptoUtils;
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

    public VariantAnnotationMongoItemWriter(MongoOperations mongoOperations) {
        this.mongoOperations = mongoOperations;
    }


 /*    public VariantAnnotationMongoItemWriter() {
        this.template = mongoTemplate();
    }

    public MongoTemplate mongoTemplate() {
        MongoTemplate mongoTemplate;
        try {
            //mongoTemplate = new MongoTemplate(new MongoClient(), pipelineOptions.getString(VariantStorageManager.DB_NAME));
            mongoTemplate = new MongoTemplate(new MongoClient(), "variants");
        } catch (UnknownHostException e) {
            throw new RuntimeException("Unable to initialize MongoDB", e);
        }
        return mongoTemplate;
    }*/

    @Override
    protected void doWrite(List<? extends VariantAnnotation> variantAnnotations) {
        for (VariantAnnotation variantAnnotation : variantAnnotations) {
            //logger.debug("Writing into mongo {}", variantAnnotation);

            //// TODO: 30/06/2016 move in constructor? 
            DBObjectToVariantAnnotationConverter converter = new DBObjectToVariantAnnotationConverter();

            String storageId = buildStorageId(variantAnnotation.getChromosome(), variantAnnotation.getStart(),
                    variantAnnotation.getReferenceAllele(), variantAnnotation.getAlternativeAllele());

            BasicDBObject find = new BasicDBObject("_id", storageId);

            //find existing variant with same Id
            DBObject existingVariantStorage = mongoOperations.getCollection("variants").findOne(find);

            //update annotation in existing variant
            if (null != existingVariantStorage){
                if(existingVariantStorage.containsField("annot")){

                    //update ConsequenceTypes
                    VariantAnnotation existingAnnotation = converter.convertToDataModelType((DBObject) existingVariantStorage.get("annot"));

                    if(null != variantAnnotation.getConsequenceTypes()){
                        existingAnnotation.getConsequenceTypes().addAll(variantAnnotation.getConsequenceTypes());
                    }

                    //update Hgvs
                    if(null != variantAnnotation.getHgvs()){
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
            mongoOperations.getCollection("variants").update(find, update);

        }
    }
    
    /**
     * From org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantConverter
     * #buildStorageId(java.lang.String, int, java.lang.String, java.lang.String)
     *
     * To avoid the initialization of:
     * - DBObjectToVariantSourceEntryConverter
     * - DBObjectToVariantConverter
     *
     */
    private String buildStorageId(String chromosome, int start, String reference, String alternate) {
        StringBuilder builder = new StringBuilder(chromosome);
        builder.append("_");
        builder.append(start);
        builder.append("_");
        if(!reference.equals("-")) {
            if(reference.length() < 50) {
                builder.append(reference);
            } else {
                builder.append(new String(CryptoUtils.encryptSha1(reference)));
            }
        }

        builder.append("_");
        if(!alternate.equals("-")) {
            if(alternate.length() < 50) {
                builder.append(alternate);
            } else {
                builder.append(new String(CryptoUtils.encryptSha1(alternate)));
            }
        }

        return builder.toString();
    }

}
