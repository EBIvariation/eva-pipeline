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
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.util.Assert;

import uk.ac.ebi.eva.commons.models.mongo.entity.Annotation;

@Document
public class AnnotationMetadata {

    public static final String IS_DEFAULT_FIELD = "is_default";

    private String id;

    @Field(Annotation.VEP_VERSION_FIELD)
    private String vepVersion;

    @Field(Annotation.VEP_CACHE_VERSION_FIELD)
    private String cacheVersion;

    @Field(IS_DEFAULT_FIELD)
    private boolean isDefault;

    AnnotationMetadata() {
        // Empty document constructor for spring-data
    }

    public AnnotationMetadata(String vepVersion, String cacheVersion, boolean isDefault) {
        Assert.hasText(vepVersion, "A non empty vepVerion is required");
        Assert.hasText(vepVersion, "A non empty cacheVersion is required");

        this.id = vepVersion + "_" + cacheVersion;
        this.vepVersion = vepVersion;
        this.cacheVersion = cacheVersion;
        this.isDefault = isDefault;
    }

    public AnnotationMetadata(String vepVersion, String cacheVersion) {
        this(vepVersion, cacheVersion, false);
    }

    public String getVepVersion() {
        return vepVersion;
    }

    public String getCacheVersion() {
        return cacheVersion;
    }

    public boolean sameVersions(AnnotationMetadata other) {
        return other.getVepVersion().equals(vepVersion) && other.getCacheVersion().equals(cacheVersion);
    }

    public boolean isDefault() {
        return isDefault;
    }

    public void setIsDefault(boolean value) {
        this.isDefault = value;
    }

}
