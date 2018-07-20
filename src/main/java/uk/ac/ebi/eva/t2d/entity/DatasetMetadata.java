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
package uk.ac.ebi.eva.t2d.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * JPA definition for Dataset table
 */
@Entity
@Table(name = "META_DATASET")
public class DatasetMetadata {

    private static final int DEFAULT_VERSION = 1;
    private static final String NULL = "NULL";
    @Id
    @Column(name = "DATASET")
    public String datsetId;

    @Column(name = "EXPTYPE")
    public String expType;

    @Column(name = "ANCESTRY")
    public String ancestry;

    @Column(name = "TECH")
    public String tech;

    @Column(name = "TBL")
    public String tableName;

    @Column(name = "CASES")
    public Integer cases;

    @Column(name = "CONTROLS")
    public Integer controls;

    @Column(name = "SUBJECTS")
    public Integer subjects;

    DatasetMetadata() {
    }

    public DatasetMetadata(String scientificGenerator, String expType, int version, String ancestry) {
        this.expType = expType;
        generateCalculatedFields(version,scientificGenerator);
        this.ancestry = ancestry;
        this.cases = -1;
        this.controls = -1;
        this.subjects = -1;
    }

    public String getDatsetId() {
        return datsetId;
    }

    private void generateCalculatedFields(int version,String scientificGenerator) {
        datsetId = expType + "_" +scientificGenerator + "_" + versionTag(version);
        tableName = datsetId.toUpperCase();
        tech = expType;
    }

    private String versionTag(int version) {
        return "mdv" + version;
    }

    public String getTableName() {
        return tableName;
    }
}
