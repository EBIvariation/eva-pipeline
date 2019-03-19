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

import uk.ac.ebi.eva.commons.models.data.AbstractVariantSourceEntry;
import uk.ac.ebi.eva.commons.models.data.IVariantSourceEntry;
import uk.ac.ebi.eva.commons.models.data.VariantStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Entry that associates a variant and a file in a variant archive. It contains
 * information related to samples, statistics and specifics of the file format.
 */
public class VariantSourceEntry extends AbstractVariantSourceEntry implements IVariantSourceEntry
{

    /**
     * Genotypes and other sample-related information. The keys are the names
     * of the samples. The values are pairs (field name, field value), such as
     * (GT, A/C).
     */
    private final List<Map<String, String>> samplesData;

    VariantSourceEntry() {
        //Spring empty constructor
        this(
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    public VariantSourceEntry(String fileId, String studyId) {
        this(fileId, studyId, null, null, null, null, null);
    }

    public VariantSourceEntry(String fileId, String studyId, String[] secondaryAlternates, String format) {
        this(fileId, studyId, secondaryAlternates, format, null, null, null);
    }

    public VariantSourceEntry(String fileId, String studyId, String[] secondaryAlternates, String format,
                              Map<String, VariantStatistics> cohortStats, Map<String, String> attributes,
                              List<Map<String, String>> samplesData) {
        super(fileId, studyId, secondaryAlternates, format, cohortStats, attributes);
        this.samplesData = new ArrayList<>();
        if (samplesData != null) {
            this.samplesData.addAll(samplesData);
        }
    }

    public List<Map<String, String>> getSamplesData() {
        return samplesData;
    }

    public String getSampleData(int sampleIndex, String field) {
        return getSampleData(sampleIndex).get(field.toUpperCase());
    }

    public Map<String, String> getSampleData(int sampleIndex) {
        return samplesData.get(sampleIndex);
    }

    /**
     * Adds information about a new sample to associate to this VariantSourceEntry.
     *
     * @param sampleData Sample information to be added
     * @return The index where the sample was inserted
     */
    public int addSampleData(Map<String, String> sampleData) {
        this.samplesData.add(sampleData);
        return this.samplesData.size() - 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VariantSourceEntry)) return false;
        if (!super.equals(o)) return false;

        VariantSourceEntry that = (VariantSourceEntry) o;

        return samplesData != null ? samplesData.equals(that.samplesData) : that.samplesData == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (samplesData != null ? samplesData.hashCode() : 0);
        return result;
    }
}
