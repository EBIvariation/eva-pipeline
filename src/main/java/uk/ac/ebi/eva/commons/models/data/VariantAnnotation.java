/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.commons.models.data;

import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.util.Assert;
import uk.ac.ebi.eva.commons.models.mongo.documents.Annotation;
import uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments.ConsequenceType;
import uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments.Score;
import uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments.Xref;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Annotations of the genomic variation
 */
public class VariantAnnotation {

    public static final String VEP_VERSION_FIELD = "vepVer";

    public static final String VEP_CACHE_VERSION_FIELD = "cacheVer";

    public static final String SIFT_FIELD = "sift";

    public static final String POLYPHEN_FIELD = "polyphen";

    public static final String SO_ACCESSION_FIELD = "so";

    public static final String XREFS_FIELD = "xrefs";

    @Field(value = VEP_VERSION_FIELD)
    private String vepVersion;

    @Field(value = VEP_CACHE_VERSION_FIELD)
    private String vepCacheVersion;

    @Field(value = SIFT_FIELD)
    private List<Double> sifts = new ArrayList<>();

    @Field(value = POLYPHEN_FIELD)
    private List<Double> polyphens = new ArrayList<>();

    @Field(value = SO_ACCESSION_FIELD)
    private Set<Integer> soAccessions = new HashSet<>();

    @Field(value = XREFS_FIELD)
    private Set<String> xrefIds = new HashSet<>();

    VariantAnnotation() {
        // Spring empty constructor
    }

    /**
     * Variant annotation constructor. Requires non empty values, otherwise throws {@link IllegalArgumentException}
     *
     * @param vepVersion      non empty value required, otherwise throws {@link IllegalArgumentException}
     * @param vepCacheVersion non empty value required, otherwise throws {@link IllegalArgumentException}
     */
    public VariantAnnotation(String vepVersion, String vepCacheVersion) {
        Assert.hasText(vepVersion, "A non empty vepVersion is required");
        Assert.hasText(vepCacheVersion, "A non empty vepCacheVersion is required");
        this.vepVersion = vepVersion;
        this.vepCacheVersion = vepCacheVersion;
    }

    VariantAnnotation(VariantAnnotation variantAnnotation) {
        this(variantAnnotation.getVepVersion(), variantAnnotation.getVepCacheVersion());
        doConcatenate(variantAnnotation);
    }

    public VariantAnnotation(Annotation annotation) {
        this(annotation.getVepVersion(), annotation.getVepCacheVersion());
        doConcatenate(annotation);
    }

    private void doConcatenate(VariantAnnotation variantAnnotation) {
        xrefIds.addAll(variantAnnotation.getXrefIds());
        for (Double siftLimit : variantAnnotation.getSifts()) {
            concatenateSiftRange(siftLimit);
        }
        for (Double polyphenLimit : variantAnnotation.getPolyphens()) {
            concatenatePolyphenRange(polyphenLimit);
        }
        soAccessions.addAll(variantAnnotation.getSoAccessions());
    }

    private void doConcatenate(Annotation annotation) {
        for (Xref xref : annotation.getXrefs()) {
            xrefIds.add(xref.getId());
        }
        for (ConsequenceType consequenceType : annotation.getConsequenceTypes()) {
            final Score sift = consequenceType.getSift();
            if (sift != null) {
                concatenateSiftRange(sift.getScore());
            }
            final Score polyphen = consequenceType.getPolyphen();
            if (polyphen != null) {
                concatenatePolyphenRange(polyphen.getScore());
            }
            final Set<Integer> soAccessions = consequenceType.getSoAccessions();
            if (soAccessions != null) {
                this.soAccessions.addAll(soAccessions);
            }
        }
    }

    private Double maxOf(Collection<Double> collection) {
        if (collection == null || collection.isEmpty()) {
            return null;
        }
        return Collections.max(collection);
    }

    private Double minOf(Collection<Double> collection) {
        if (collection == null || collection.isEmpty()) {
            return null;
        }
        return Collections.min(collection);
    }

    private void concatenateRange(Collection<Double> collection, Double score) {
        Double min = minOf(collection);
        Double max = maxOf(collection);
        if(min == null || max == null){
            setRange(collection, score, score);
        } else if (score < min){
            setRange(collection, score, max);
        } else if (score > max){
            setRange(collection, min, score);
        }
    }

    private void setRange(Collection<Double> collection, Double minScore, Double maxScore) {
        collection.clear();
        collection.add(minScore);
        collection.add(maxScore);
    }

    private void concatenateSiftRange(Double score) {
        concatenateRange(sifts, score);
    }

    private void concatenatePolyphenRange(Double score) {
        concatenateRange(polyphens, score);
    }

    public void addSift(Double sift) {
        this.sifts.add(sift);
    }

    public void addSifts(Collection<Double> sifts) {
        this.sifts.addAll(sifts);
    }

    public void addPolyphen(Double polyphen) {
        this.polyphens.add(polyphen);
    }

    public void addPolyphens(Collection<Double> polyphens) {
        this.polyphens.addAll(polyphens);
    }

    public void addXrefIds(Set<String> xrefIds) {
        this.xrefIds.addAll(xrefIds);
    }

    public void addsoAccessions(Set<Integer> soAccessions) {
        this.soAccessions.addAll(soAccessions);
    }

    public List<Double> getSifts() {
        return sifts;
    }

    public List<Double> getPolyphens() {
        return polyphens;
    }

    public Set<Integer> getSoAccessions() {
        return soAccessions;
    }

    public Set<String> getXrefIds() {
        return xrefIds;
    }

    public String getVepVersion() {
        return vepVersion;
    }

    public String getVepCacheVersion() {
        return vepCacheVersion;
    }

    public VariantAnnotation concatenate(Annotation annotation) {
        VariantAnnotation temp = new VariantAnnotation(this);
        temp.doConcatenate(annotation);
        return temp;
    }

    public VariantAnnotation concatenate(VariantAnnotation annotation) {
        VariantAnnotation temp = new VariantAnnotation(this);
        temp.doConcatenate(annotation);
        return temp;
    }
}
