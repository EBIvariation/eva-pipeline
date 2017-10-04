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
package uk.ac.ebi.eva.t2d.utils;

import org.opencb.biodata.models.variant.annotation.ConsequenceType;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.mongo.entity.Annotation;
import uk.ac.ebi.eva.pipeline.model.EnsemblVariant;
import uk.ac.ebi.eva.t2d.entity.VariantInfo;

import java.util.List;
import java.util.Set;

public class VariantUtils {

    private static final String RS = "rs";
    private static final String SS = "ss";


    public static String getVariantId(Variant entity) {
        EnsemblVariant ensemblVariant = new EnsemblVariant(entity.getChromosome(), entity.getStart(),
                entity.getEnd(), entity.getReference(), entity.getAlternate());
        return generateVariantId(ensemblVariant.getChr(), ensemblVariant.getStart(),
                ensemblVariant.getReference(), ensemblVariant.getAlternate());
    }

    public static String generateVariantId(String chromosome, int start, String reference, String alternate) {
        return chromosome + "_" + start + "_" + reference + "_" + alternate;
    }

    public static String getDbsnpId(Variant variant) {
        Set<String> ids = variant.getIds();
        String temptativeId = null;
        for (String id : ids) {
            if (id.startsWith(RS)) {
                return id;
            }
            if (id.startsWith(SS)) {
                temptativeId = id;
            }
        }
        return temptativeId;
    }

    public static String getConsequence(List<ConsequenceType> consequenceTypes) {
        if (consequenceTypes.isEmpty()) {
            return null;
        }
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(getSoNames(consequenceTypes.get(0)));

        for (int i = 0; i < consequenceTypes.size(); i++) {
            stringBuilder.append(",").append(getSoNames(consequenceTypes.get(0)));
        }
        return stringBuilder.toString();
    }

    private static String getSoNames(ConsequenceType consequenceType) {
        if (consequenceType.getSoTerms().isEmpty()) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(consequenceType.getSoTerms().get(0).getSoName());

        for (int i = 1; i < consequenceType.getSoTerms().size(); i++) {
            stringBuilder.append(",").append(consequenceType.getSoTerms().get(i).getSoName());
        }
        return stringBuilder.toString();
    }

    public static String getClosestGene(VariantAnnotation annotation) {
        // TODO
        return null;
    }

    public static String getGene(VariantAnnotation annotation) {
        // TODO
        return null;
    }

    public static String getSiftPrediction(VariantAnnotation annotation) {
        if (annotation.getProteinSubstitutionScores() != null && annotation.getProteinSubstitutionScores()
                .getSiftEffect() != null) {
            return annotation.getProteinSubstitutionScores().getSiftEffect().toString().toLowerCase();
        }
        return null;
    }

    public static String getPolyphenPrediction(VariantAnnotation annotation) {
        if (annotation.getProteinSubstitutionScores() != null && annotation.getProteinSubstitutionScores()
                .getPolyphenEffect() != null) {
            return annotation.getProteinSubstitutionScores().getPolyphenEffect().toString().toLowerCase();
        }
        return null;
    }

}
