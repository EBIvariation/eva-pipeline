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

import java.util.HashSet;
import java.util.Set;

/**
 * Base implementation of the annotation model class.
 */
public class Annotation implements IAnnotation {

    private String chromosome;

    private long start;

    private long end;

    private String vepVersion;

    private String vepCacheVersion;

    private Set<ConsequenceType> consequenceTypes;

    private Set<Xref> xrefs;

    Annotation() {
        //Empty spring constructor
        this(null, -1, -1, null, null, null, null);
    }

    public Annotation(IAnnotation annotation) {
        this(
                annotation.getChromosome(),
                annotation.getStart(),
                annotation.getEnd(),
                annotation.getVepVersion(),
                annotation.getVepCacheVersion(),
                annotation.getXrefs(),
                annotation.getConsequenceTypes()
        );
    }

    public Annotation(String chromosome, long start, long end, String vepVersion, String vepCacheVersion,
                      Set<? extends IXref> xrefs, Set<? extends IConsequenceType> consequenceTypes) {
        this.chromosome = chromosome;
        this.start = start;
        this.end = end;
        this.vepVersion = vepVersion;
        this.vepCacheVersion = vepCacheVersion;
        this.xrefs = new HashSet<>();
        if (xrefs != null) {
            xrefs.forEach(xref -> this.xrefs.add(new Xref(xref)));
        }
        this.consequenceTypes = new HashSet<>();
        if (consequenceTypes != null) {
            consequenceTypes.forEach(consequence -> this.consequenceTypes.add(new ConsequenceType(consequence)));
        }
    }

    @Override
    public String getChromosome() {
        return chromosome;
    }

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public long getEnd() {
        return end;
    }

    @Override
    public String getVepVersion() {
        return vepVersion;
    }

    @Override
    public String getVepCacheVersion() {
        return vepCacheVersion;
    }

    @Override
    public Set<ConsequenceType> getConsequenceTypes() {
        return consequenceTypes;
    }

    @Override
    public Set<Xref> getXrefs() {
        return xrefs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Annotation)) return false;

        Annotation that = (Annotation) o;

        if (start != that.start) return false;
        if (end != that.end) return false;
        if (chromosome != null ? !chromosome.equals(that.chromosome) : that.chromosome != null) return false;
        if (vepVersion != null ? !vepVersion.equals(that.vepVersion) : that.vepVersion != null) return false;
        if (vepCacheVersion != null ? !vepCacheVersion.equals(that.vepCacheVersion) : that.vepCacheVersion != null)
            return false;
        if (consequenceTypes != null ? !consequenceTypes.equals(that.consequenceTypes) : that.consequenceTypes != null)
            return false;
        return xrefs != null ? xrefs.equals(that.xrefs) : that.xrefs == null;

    }

    @Override
    public int hashCode() {
        int result = chromosome.hashCode();
        result = 31 * result + (int) (start ^ (start >>> 32));
        result = 31 * result + (int) (end ^ (end >>> 32));
        result = 31 * result + vepVersion.hashCode();
        result = 31 * result + vepCacheVersion.hashCode();
        result = 31 * result + consequenceTypes.hashCode();
        result = 31 * result + xrefs.hashCode();
        return result;
    }
}
