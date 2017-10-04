/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
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

@Entity
@Table(name = "DATASET")
public class DatasetMetadata {

    private static final int DEFAULT_VERSION = 1;
    private static final String NULL = "NULL";
    @Id
    @Column(name = "ID")
    public String id;

    @Column(name = "EXP")
    public String experimentName;

    @Column(name = "SG")
    public String scientificGenerator;

    @Column(name = "EXPTYPE")
    public String expType;

    @Column(name = "VER")
    public String ver;

    @Column(name = "PARENT")
    public String parent;

    @Column(name = "ANCESTRY")
    public String ancestry;

    @Column(name = "TECH")
    public String tech;

    @Column(name = "TBL")
    public String tableName;

    @Column(name = "SORT")
    public Double sort;

    @Column(name = "CASES")
    public Integer cases;

    @Column(name = "CONTROLS")
    public Integer controls;

    @Column(name = "SUBJECTS")
    public Integer subjects;

    DatasetMetadata() {
    }

    public DatasetMetadata(String scientificGenerator, String expType, int version, String ancestry, int release) {
        this.scientificGenerator = scientificGenerator;
        this.expType = expType;
        setVersion(release);
        generateCalculatedFields(version);
        this.ancestry = ancestry;
        this.cases = -1;
        this.controls = -1;
        this.parent = "Root";
        this.sort = Double.valueOf(0);
        this.subjects = -1;
    }

    public String getId() {
        return id;
    }

    private void generateCalculatedFields(int version) {
        experimentName = expType + "_" + scientificGenerator;
        id = experimentName + "_" + versionTag(version);
        tableName = id.toUpperCase();
        tech = expType;
    }

    private String versionTag(int version) {
        return "mdv" + version;
    }

    private void setVersion(int version) {
        this.ver = versionTag(version);
    }

    public String getTableName() {
        return tableName;
    }

    public String getVer() {
        return ver;
    }
}
