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

import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Annotations of the genomic variation
 */
@Document
public class VariantAnnotation {
    @Field(value = AnnotationFieldNames.ENSEMBL_VERSION_FIELD)
    private String ensemblVersion;

    @Field(value = AnnotationFieldNames.VEP_CACHE_VERSION_FIELD)
    private String vepCacheVersion;

    @Field(value = AnnotationFieldNames.SIFT_FIELD)
    private Set<Double> sifts = new HashSet<>();

    @Field(value = AnnotationFieldNames.POLYPHEN_FIELD)
    private Set<Double> polyphens = new HashSet<>();

    @Field(value = AnnotationFieldNames.SO_ACCESSION_FIELD)
    private Set<Integer> soAccessions = new HashSet<>();

    @Field(value = AnnotationFieldNames.XREFS_FIELD)
    private Set<String> xrefIds = new HashSet<>();

    /**
     * Make sure to specify the ensemblVersion and vepCacheVersion
     * @param ensemblVersion
     * @param vepCacheVersion
     */
    public VariantAnnotation(String ensemblVersion, String vepCacheVersion) {
        Assert.notNull(ensemblVersion);
        Assert.notNull(vepCacheVersion);
        this.ensemblVersion = ensemblVersion;
        this.vepCacheVersion = vepCacheVersion;
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

    public Set<Double> getSifts() {
        return sifts;
    }

    public Set<Double> getPolyphens() {
        return polyphens;
    }

    public Set<Integer> getSoAccessions() {
        return soAccessions;
    }

    public Set<String> getXrefIds() {
        return xrefIds;
    }

    public String getEnsemblVersion() {
        return ensemblVersion;
    }

    public String getVepCacheVersion() {
        return vepCacheVersion;
    }
}
