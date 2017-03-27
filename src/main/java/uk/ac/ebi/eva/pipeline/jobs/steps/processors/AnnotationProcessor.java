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

package uk.ac.ebi.eva.pipeline.jobs.steps.processors;

import com.mongodb.DBObject;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.eva.pipeline.model.VariantWrapper;

/**
 * Convert a {@link DBObject} into {@link VariantWrapper}
 * Any extra filter, check, validation... should be placed here
 */
public class AnnotationProcessor implements ItemProcessor<DBObject, VariantWrapper> {
    private static final Logger logger = LoggerFactory.getLogger(AnnotationProcessor.class);

    private DBObjectToVariantConverter converter;

    public AnnotationProcessor() {
        converter = new DBObjectToVariantConverter();
    }

    @Override
    public VariantWrapper process(DBObject object) throws Exception {
        //logger.debug("Convert {} into a VariantWrapper", object);
        Variant variant = converter.convertToDataModelType(object);

        return new VariantWrapper(variant);
    }
}
