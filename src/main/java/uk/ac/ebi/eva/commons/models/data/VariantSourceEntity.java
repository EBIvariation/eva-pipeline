/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.biodata.models.variant.stats.VariantGlobalStats;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 * Represents a file (VariantSource) in a database.
 * <p>
 * TODO jmmut: VariantSource also has pedigree
 */
@Document
public class VariantSourceEntity {

    @Field(value = "fname")
    private String fileName;

    @Field(value = "fid")
    private String fileId;

    @Field(value = "sid")
    private String studyId;

    @Field(value = "sname")
    private String studyName;

    @Field(value = "samp")
    private Map<String, Integer> samplesPosition;

    @Field(value = "meta")
    private Map<String, Object> metadata;

    @Field(value = "stype")
    private VariantStudy.StudyType type;

    @Field(value = "st")
    private VariantGlobalStats stats;

    @Field(value = "date")
    private Date date;

    @Field(value = "aggregation")
    private VariantSource.Aggregation aggregation;

    public VariantSourceEntity(String fileName, String fileId, String studyId, String studyName,
                               Map<String, Integer> samplesPosition,
                               Map<String, Object> metadata, VariantStudy.StudyType type,
                               VariantGlobalStats stats,
                               VariantSource.Aggregation aggregation) {
        this.fileName = fileName;
        this.fileId = fileId;
        this.studyId = studyId;
        this.studyName = studyName;
        this.samplesPosition = samplesPosition;
        this.metadata = metadata;
        this.type = type;
        this.stats = stats;
        this.aggregation = aggregation;
        this.date = Calendar.getInstance().getTime();
    }

    public VariantSourceEntity(VariantSource source) {
        this(source.getFileName(), source.getFileId(), source.getStudyId(), source.getStudyName(),
             source.getSamplesPosition(), source.getMetadata(), source.getType(), source.getStats(),
             source.getAggregation());
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public String getStudyId() {
        return studyId;
    }

    public void setStudyId(String studyId) {
        this.studyId = studyId;
    }

    public String getStudyName() {
        return studyName;
    }

    public void setStudyName(String studyName) {
        this.studyName = studyName;
    }

    public Map<String, Integer> getSamplesPosition() {
        return samplesPosition;
    }

    public void setSamplesPosition(Map<String, Integer> samplesPosition) {
        this.samplesPosition = samplesPosition;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public VariantStudy.StudyType getType() {
        return type;
    }

    public void setType(VariantStudy.StudyType type) {
        this.type = type;
    }

    public VariantGlobalStats getStats() {
        return stats;
    }

    public void setStats(VariantGlobalStats stats) {
        this.stats = stats;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public VariantSource.Aggregation getAggregation() {
        return aggregation;
    }

    public void setAggregation(VariantSource.Aggregation aggregation) {
        this.aggregation = aggregation;
    }
}
