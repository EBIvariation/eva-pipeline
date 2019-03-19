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
package uk.ac.ebi.eva.commons.models.data.pipeline;

import uk.ac.ebi.eva.commons.models.data.AbstractVariant;
import uk.ac.ebi.eva.commons.models.data.IVariantSourceEntry;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Basic implementation to implement a variant attuned to use in the pipeline
 */
public class Variant extends AbstractVariant
{

    /**
     * Information specific to each file the variant was read from, such as samples or statistics.
     */
    private final Map<String, VariantSourceEntry> sourceEntries;

    Variant() {
        //Spring empty constructor
        super();
        sourceEntries = new HashMap<>();
    }

    public Variant(String chromosome, long start, long end, String reference, String alternate) {
        super(chromosome, start, end, reference, alternate);
        sourceEntries = new HashMap<>();
    }

    public void addSourceEntry(VariantSourceEntry sourceEntry) {
        this.sourceEntries.put(getSourceEntryIndex(sourceEntry.getStudyId(),
                sourceEntry.getFileId()), sourceEntry);
    }

    public void addSourceEntries(Collection<VariantSourceEntry> variantSourceEntries) {
        this.sourceEntries.clear();
        variantSourceEntries.forEach(variantSourceEntry ->
                sourceEntries.put(getSourceEntryIndex(variantSourceEntry), variantSourceEntry));
    }

    private String getSourceEntryIndex(IVariantSourceEntry variantSourceEntries) {
        return getSourceEntryIndex(variantSourceEntries.getStudyId(), variantSourceEntries.getFileId());
    }

    private String getSourceEntryIndex(String studyId, String fileId) {
        return studyId + "_" + fileId;
    }

    @Override
    public Collection<VariantSourceEntry> getSourceEntries() {
        return Collections.unmodifiableCollection(sourceEntries.values());
    }

    @Override
    public VariantSourceEntry getSourceEntry(String fileId, String studyId) {
        return sourceEntries.get(getSourceEntryIndex(studyId, fileId));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Variant)) return false;
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
