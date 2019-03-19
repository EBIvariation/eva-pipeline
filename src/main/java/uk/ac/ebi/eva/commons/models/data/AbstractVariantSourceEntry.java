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

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Entry that associates a variant and a file in a variant archive. It contains
 * information related to samples, statistics and specifics of the file format.
 */
public abstract class AbstractVariantSourceEntry implements IVariantSourceEntry {

    public static final String DEFAULT_COHORT = "ALL";

    /**
     * Unique identifier of the archived file.
     */
    private String fileId;

    /**
     * Unique identifier of the study containing the archived file.
     */
    private String studyId;

    /**
     * Alternate alleles that appear along with a variant alternate.
     */
    private String[] secondaryAlternates;

    /**
     * Fields stored for each sample.
     */
    private String format;

    /**
     * Statistics of the genomic variation, such as its alleles/genotypes count
     * or its minimum allele frequency, grouped by cohort name.
     */
    private Map<String, VariantStatistics> cohortStats;

    /**
     * Optional attributes that probably depend on the format of the file the
     * variant was initially read from.
     */
    private Map<String, String> attributes;

    public AbstractVariantSourceEntry(String fileId, String studyId, String[] secondaryAlternates, String format,
                                      Map<String, VariantStatistics> cohortStats, Map<String, String> attributes) {
        this.fileId = fileId;
        this.studyId = studyId;
        if (secondaryAlternates != null) {
            this.secondaryAlternates = Arrays.copyOf(secondaryAlternates, secondaryAlternates.length);
        } else {
            this.secondaryAlternates = new String[]{};
        }
        this.format = format;
        this.cohortStats = new HashMap<>();
        if (cohortStats != null) {
            this.cohortStats.putAll(cohortStats);
        }
        this.attributes = new HashMap<>();
        if (attributes != null) {
            this.attributes.putAll(attributes);
        }
    }


    @Override
    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    @Override
    public String getStudyId() {
        return studyId;
    }

    public void setStudyId(String studyId) {
        this.studyId = studyId;
    }

    @Override
    public String[] getSecondaryAlternates() {
        return secondaryAlternates;
    }

    public void setSecondaryAlternates(String[] secondaryAlternates) {
        this.secondaryAlternates = secondaryAlternates;
    }

    @Override
    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    @Override
    public VariantStatistics getStats() {
        return cohortStats.get(DEFAULT_COHORT);
    }

    @Override
    public void setStats(VariantStatistics stats) {
        this.cohortStats = new LinkedHashMap<>(1);
        this.cohortStats.put(DEFAULT_COHORT, stats);
    }

    public VariantStatistics getCohortStats(String cohortName) {
        return cohortStats.get(cohortName);
    }

    public void setCohortStats(String cohortName, VariantStatistics stats) {
        this.cohortStats.put(cohortName, stats);
    }

    public Map<String, VariantStatistics> getCohortStats() {
        return cohortStats;
    }

    public void setCohortStats(Map<String, VariantStatistics> cohortStats) {
        this.cohortStats = cohortStats;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public String getAttribute(String key) {
        return this.attributes.get(key);
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public void addAttribute(String key, String value) {
        this.attributes.put(key, value);
    }

    public boolean hasAttribute(String key) {
        return this.attributes.containsKey(key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AbstractVariantSourceEntry)) return false;

        AbstractVariantSourceEntry that = (AbstractVariantSourceEntry) o;

        if (fileId != null ? !fileId.equals(that.fileId) : that.fileId != null) return false;
        if (studyId != null ? !studyId.equals(that.studyId) : that.studyId != null) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(secondaryAlternates, that.secondaryAlternates)) return false;
        if (format != null ? !format.equals(that.format) : that.format != null) return false;
        if (cohortStats != null ? !cohortStats.equals(that.cohortStats) : that.cohortStats != null) return false;
        return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
    }

    @Override
    public int hashCode() {
        int result = fileId != null ? fileId.hashCode() : 0;
        result = 31 * result + (studyId != null ? studyId.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(secondaryAlternates);
        result = 31 * result + (format != null ? format.hashCode() : 0);
        result = 31 * result + (cohortStats != null ? cohortStats.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        return result;
    }

}
