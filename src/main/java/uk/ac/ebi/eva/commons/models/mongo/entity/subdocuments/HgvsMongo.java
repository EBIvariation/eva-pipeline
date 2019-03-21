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
package uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments;

import org.springframework.data.mongodb.core.mapping.Field;

/**
 * Mongo database representation of HGVS field.
 */
public class HgvsMongo {

    private static final String TYPE_FIELD = "type";

    private static final String NAME_FIELD = "name";

    @Field(TYPE_FIELD)
    private String type;

    @Field(NAME_FIELD)
    private String name;

    HgvsMongo(){
        //Empty constructor for spring
    }

    public HgvsMongo(String type, String name) {
        this.type = type;
        this.name = name;
    }
}
