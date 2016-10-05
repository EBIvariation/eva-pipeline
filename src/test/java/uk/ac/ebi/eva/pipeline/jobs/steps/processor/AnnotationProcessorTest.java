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
package uk.ac.ebi.eva.pipeline.jobs.steps.processor;

import com.mongodb.DBObject;
import org.junit.Test;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.eva.pipeline.jobs.steps.processors.AnnotationProcessor;
import uk.ac.ebi.eva.pipeline.model.VariantWrapper;
import uk.ac.ebi.eva.test.data.VariantData;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

import static org.junit.Assert.assertEquals;

/**
 * {@link AnnotationProcessor}
 * input: a DBObject
 * output: a VariantWrapper
 */
public class AnnotationProcessorTest {

    @Test
    public void shouldConvertAllFieldsInVariant() throws Exception {
        DBObject dbo = JobTestUtils.constructDbo(VariantData.getVariantWithoutAnnotation());

        ItemProcessor<DBObject, VariantWrapper> processor =  new AnnotationProcessor();
        VariantWrapper variant = processor.process(dbo);
        assertEquals("+", variant.getStrand());
        assertEquals("20", variant.getChr());
        assertEquals("G/A", variant.getRefAlt());
        assertEquals(60343, variant.getEnd());
        assertEquals(60343, variant.getStart());
    }

}
