/*
 * Copyright 2015-2017 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.commons.models.metadata;

import org.springframework.data.mongodb.core.mapping.Document;

@Document
public class AnnotationMetadata {

    private String id;

    private String vepVersion;

    private String cacheVersion;

    AnnotationMetadata() {
    }

    public AnnotationMetadata(String vepVersion, String cacheVersion) {
        this.id = vepVersion + "_" + cacheVersion;
        this.vepVersion = vepVersion;
        this.cacheVersion = cacheVersion;
    }

    public AnnotationMetadata(String id, String vepVersion, String cacheVersion) {
        this.id = id;
        this.vepVersion = vepVersion;
        this.cacheVersion = cacheVersion;
    }

    public String getVepVersion() {
        return vepVersion;
    }

    public void setVepVersion(String vepVersion) {
        this.vepVersion = vepVersion;
    }

    public String getCacheVersion() {
        return cacheVersion;
    }

    public void setCacheVersion(String cacheVersion) {
        this.cacheVersion = cacheVersion;
    }
}
