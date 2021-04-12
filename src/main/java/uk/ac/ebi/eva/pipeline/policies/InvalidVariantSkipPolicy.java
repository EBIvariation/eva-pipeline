/*
 *
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
 *
 */
package uk.ac.ebi.eva.pipeline.policies;

import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.file.FlatFileParseException;

import uk.ac.ebi.eva.pipeline.exception.IncompleteInformationException;
import uk.ac.ebi.eva.pipeline.exception.NonVariantException;

/**
 * Policy for skipping {@link FlatFileParseException} caused by {@link NonVariantException} or
 * {@link IncompleteInformationException}
 */
public class InvalidVariantSkipPolicy implements SkipPolicy {

    @Override
    public boolean shouldSkip(Throwable exception, int skipCount) throws SkipLimitExceededException {
        if (exception instanceof FlatFileParseException
                && (exception.getCause() instanceof NonVariantException
                || exception.getCause() instanceof IncompleteInformationException)) {
            return true;
        }
        return false;
    }
}
