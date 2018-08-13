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

/**
 * JPA definition for the Dataset and Versions
 */
@Entity
@Table(name = "META_MDV")
public class DatasetVersionMetadata {

    @Id
    @Column(name = "ID")
    public String id;

    @Column(name = "EXP")
    public String experimentName;

    @Column(name = "SG")
    public String scientificGenerator;

    @Column(name = "VER")
    public String ver;

    @Column(name = "PARENT")
    public String parent;

    @Column(name = "DATASET")
    public String datasetName;

    @Column(name = "SORT")
    public Double sort;

    DatasetVersionMetadata() {
    }

    public DatasetVersionMetadata(String scientificGenerator, String expType, int version, int release) {
        this.scientificGenerator = scientificGenerator;
        setVersion(release);
        generateCalculatedFields(version, expType);
        this.parent = "Root";
        this.sort = Double.valueOf(0);
    }

    public String getId() {
        return id;
    }

    private void generateCalculatedFields(int version, String expType) {
        experimentName = expType + "_" + scientificGenerator;
        id = experimentName + "_" + versionTag(version);
        datasetName = id;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public String getVer() {
        return ver;
    }

    private String versionTag(int version) {
        return "mdv" + version;
    }

    private void setVersion(int version) {
        this.ver = versionTag(version);
    }
}
