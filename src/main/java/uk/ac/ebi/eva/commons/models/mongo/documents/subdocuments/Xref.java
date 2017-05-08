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
package uk.ac.ebi.eva.commons.models.mongo.documents.subdocuments;

import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import uk.ac.ebi.eva.commons.models.data.AnnotationFieldNames;

/**
 * From org.opencb.biodata.models.variant.annotation.Xref
 */
@Document
public class Xref {

    @Field(value = AnnotationFieldNames.XREF_ID_FIELD)
    private String id;

    @Field(value = AnnotationFieldNames.XREF_SOURCE_FIELD)
    private String src;

    public Xref(String id, String src) {
        this.id = id;
        this.src = src;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Xref xref = (Xref) o;

        if (id != null ? !id.equals(xref.id) : xref.id != null) return false;
        return src != null ? src.equals(xref.src) : xref.src == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (src != null ? src.hashCode() : 0);
        return result;
    }
}
