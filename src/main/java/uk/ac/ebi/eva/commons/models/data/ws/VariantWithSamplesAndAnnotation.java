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
package uk.ac.ebi.eva.commons.models.data.ws;

import uk.ac.ebi.eva.commons.models.data.AbstractVariant;
import uk.ac.ebi.eva.commons.models.data.Annotation;
import uk.ac.ebi.eva.commons.models.data.IVariant;
import uk.ac.ebi.eva.commons.models.data.IVariantSourceEntry;

import java.util.*;

/**
 * A mutation in the genome, defined as a change from a reference to an alternate allele in a certain position of
 * said genome.
 */
public class VariantWithSamplesAndAnnotation extends AbstractVariant {

    /**
     * Information specific to each file the variant was read from, such as samples or statistics.
     */
    private final Map<String, VariantSourceEntryWithSampleNames> sourceEntries;

    /**
     * Annotations of the genomic variation.
     */
    private Annotation annotation;

    VariantWithSamplesAndAnnotation() {
        super();
        sourceEntries = new HashMap<>();
    }

    public VariantWithSamplesAndAnnotation(String chromosome, long start, long end, String reference, String alternate,
                                           String mainId) {
        super(chromosome, start, end, reference, alternate, mainId);
        sourceEntries = new HashMap<>();
    }

    public VariantWithSamplesAndAnnotation(IVariant variant, List<String> sampleNames) {
        super(variant.getChromosome(), variant.getStart(), variant.getEnd(), variant.getReference(),
              variant.getAlternate(), variant.getMainId());
        this.sourceEntries = new HashMap<>();
        for (IVariantSourceEntry entry : variant.getSourceEntries()) {
            VariantSourceEntryWithSampleNames entryWithSampleNames = new VariantSourceEntryWithSampleNames(entry,
                                                                                                           sampleNames);
            this.sourceEntries.put(getSourceEntryIndex(entryWithSampleNames), entryWithSampleNames);
        }
    }


    public void setAnnotation(Annotation annotation) {
        this.annotation = annotation;
    }

    public Annotation getAnnotation() {
        return annotation;
    }

    public void addSourceEntry(VariantSourceEntryWithSampleNames sourceEntry) {
        this.sourceEntries.put(getSourceEntryIndex(sourceEntry.getStudyId(), sourceEntry.getFileId()), sourceEntry);
    }

    public void addSourceEntries(Collection<VariantSourceEntryWithSampleNames> variantSourceEntries) {
        this.sourceEntries.clear();
        variantSourceEntries.forEach(variantSourceEntry ->
                sourceEntries.put(getSourceEntryIndex(variantSourceEntry), variantSourceEntry));
    }

    private String getSourceEntryIndex(VariantSourceEntryWithSampleNames variantSourceEntries) {
        return getSourceEntryIndex(variantSourceEntries.getStudyId(), variantSourceEntries.getFileId());
    }

    private String getSourceEntryIndex(String studyId, String fileId) {
        return studyId + "_" + fileId;
    }

    @Override
    public Collection<VariantSourceEntryWithSampleNames> getSourceEntries() {
        return Collections.unmodifiableCollection(sourceEntries.values());
    }

    @Override
    public VariantSourceEntryWithSampleNames getSourceEntry(String fileId, String studyId) {
        return sourceEntries.get(getSourceEntryIndex(studyId, fileId));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VariantWithSamplesAndAnnotation)) return false;
        if (!super.equals(o)) return false;

        VariantWithSamplesAndAnnotation that = (VariantWithSamplesAndAnnotation) o;

        if (sourceEntries != null ? !sourceEntries.equals(that.sourceEntries) : that.sourceEntries != null)
            return false;
        return annotation != null ? annotation.equals(that.annotation) : that.annotation == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (sourceEntries != null ? sourceEntries.hashCode() : 0);
        result = 31 * result + (annotation != null ? annotation.hashCode() : 0);
        return result;
    }
}
