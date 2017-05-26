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

import java.util.HashSet;
import java.util.Set;

/**
 * Mongo database representation of a Variant AT field.
 */
public class VariantAt {

    private static final String CHUNK_IDS_FIELD = "chunkIds";

    @Field(CHUNK_IDS_FIELD)
    private Set<String> chunkIds;

    VariantAt(){
        //Empty constructor for spring
    }

    public VariantAt(String chunkSmall, String chunkBig) {
        chunkIds = new HashSet<>();
        chunkIds.add(chunkSmall);
        chunkIds.add(chunkBig);
    }
}
