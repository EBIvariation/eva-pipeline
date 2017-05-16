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
package uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments;

import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.util.Assert;
import uk.ac.ebi.eva.commons.models.mongo.documents.Annotation;

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

    public static final String VEP_VERSION_FIELD = "vepv";

    public static final String VEP_CACHE_VERSION_FIELD = "cachev";

    public static final String SIFT_FIELD = "sift";

    public static final String POLYPHEN_FIELD = "polyphen";

    public static final String SO_ACCESSION_FIELD = "so";

    public static final String XREFS_FIELD = "xrefs";

    @Field(value = VEP_VERSION_FIELD)
    private String vepVersion;

    @Field(value = VEP_CACHE_VERSION_FIELD)
    private String vepCacheVersion;

    @Field(value = SIFT_FIELD)
    private List<Double> sifts;

    @Field(value = POLYPHEN_FIELD)
    private List<Double> polyphens;

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
        Assert.hasText(vepVersion);
        Assert.hasText(vepCacheVersion);
        this.vepVersion = vepVersion;
        this.vepCacheVersion = vepCacheVersion;
    }

    /**
     * Private copy constructor
     *
     * @param variantAnnotation
     */
    private VariantAnnotation(VariantAnnotation variantAnnotation) {
        this(variantAnnotation.getVepVersion(), variantAnnotation.getVepCacheVersion());
        doConcatenate(variantAnnotation);
    }

    public VariantAnnotation(Annotation annotation) {
        this(annotation.getVepVersion(), annotation.getVepCacheVersion());
        doConcatenate(annotation);
    }

    private void doConcatenate(VariantAnnotation variantAnnotation) {
        if (variantAnnotation.getXrefIds() != null) {
            addXrefIds(variantAnnotation.getXrefIds());
        }
        if (variantAnnotation.getSifts() != null) {
            for (Double siftLimit : variantAnnotation.getSifts()) {
                concatenateSiftRange(siftLimit);
            }
        }
        if (variantAnnotation.getPolyphens() != null) {
            for (Double polyphenLimit : variantAnnotation.getPolyphens()) {
                concatenatePolyphenRange(polyphenLimit);
            }
        }
        if (variantAnnotation.getSoAccessions() != null) {
            addsoAccessions(variantAnnotation.getSoAccessions());
        }
    }

    private void doConcatenate(Annotation annotation) {
        for (Xref xref : annotation.getXrefs()) {
            addXrefId(xref.getId());
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
                addsoAccessions(soAccessions);
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
        if (min == null || max == null) {
            setRange(collection, score, score);
        } else if (score < min) {
            setRange(collection, score, max);
        } else if (score > max) {
            setRange(collection, min, score);
        }
    }

    private void setRange(Collection<Double> collection, Double minScore, Double maxScore) {
        collection.clear();
        collection.add(minScore);
        collection.add(maxScore);
    }

    private void concatenateSiftRange(Double score) {
        if (sifts == null) {
            sifts = new ArrayList<>();
        }
        concatenateRange(sifts, score);
    }

    private void concatenatePolyphenRange(Double score) {
        if (polyphens == null) {
            polyphens = new ArrayList<>();
        }
        concatenateRange(polyphens, score);
    }

    private void addXrefId(String id) {
        if(xrefIds==null){
            xrefIds = new HashSet<>();
        }
        xrefIds.add(id);
    }

    private void addXrefIds(Set<String> ids) {
        if(xrefIds==null){
            xrefIds = new HashSet<>();
        }
        xrefIds.addAll(ids);
    }

    private void addsoAccessions(Set<Integer> soAccessions) {
        if (this.soAccessions == null) {
            this.soAccessions = new HashSet<>();
        }
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

    /**
     * Concatenate two VariantAnnotations in a new one. This method returns a new instance of VariantAnnotation with
     * the concatenation of xrefIds and soAccessions. This concatenation also has new values for the ranges of
     * polyphen and sift values to include the values expressend in the concatenated VariantAnnotation.
     *
     * @param annotation
     * @return
     */
    public VariantAnnotation concatenate(VariantAnnotation annotation) {
        VariantAnnotation temp = new VariantAnnotation(this);
        temp.doConcatenate(annotation);
        return temp;
    }
}
