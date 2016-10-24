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
package uk.ac.ebi.eva.pipeline.io.mappers;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;

import uk.ac.ebi.eva.test.data.VepOutputContent;

/**
 * {@link AnnotationLineMapper}
 * input: an annotation line from VEP
 * output: a VariantAnnotation with at least: consequence types
 */
public class AnnotationLineMapperTest {

    @Test
    public void shouldReadAllFieldsInVepOutput() throws Exception {
        AnnotationLineMapper lineMapper = new AnnotationLineMapper();
        for (String annotLine : VepOutputContent.vepOutputContent.split("\n")) {
            VariantAnnotation variantAnnotation = lineMapper.mapLine(annotLine, 0);
            assertNotNull(variantAnnotation.getConsequenceTypes());
        }
    }

}
