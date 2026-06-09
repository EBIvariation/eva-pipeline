/*
 * Copyright 2023 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.pipeline.configuration;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.commons.mongodb.entities.AnnotationMetadataMongo;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;

@ExtendWith(SpringExtension.class)
public class MongoTemplateTest {
    @MockBean
    private MongoTemplate mongoTemplate;

    @BeforeEach
    public void setUp() {

    }

    @Test
    public void testMongoTemplateWriteResultChecking() {
        AnnotationMetadataMongo annotationMetadata = new AnnotationMetadataMongo("vep_1", "vep_cache_1");
        annotationMetadata.setDefaultVersion(true);
        doThrow(new DataAccessException("Simulated exception") {
        })
                .when(mongoTemplate).save(annotationMetadata, "AnnotationMetadata");

        assertThrows(DataAccessException.class, () -> mongoTemplate.save(annotationMetadata, "AnnotationMetadata"));
    }
}
