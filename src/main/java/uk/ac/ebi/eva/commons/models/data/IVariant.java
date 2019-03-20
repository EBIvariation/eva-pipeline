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
package uk.ac.ebi.eva.commons.models.data;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Interface that describes the basic common information of the variant model
 */
public interface IVariant
{

    VariantType getType();

    String getChromosome();

    long getStart();

    long getEnd();

    int getLength();

    String getReference();

    String getAlternate();

    Set<String> getIds();

    default String getMainId() { return null; }

    /** @deprecated this field is temporary, use getIds or getMainId instead */
    @Deprecated
    Set<String> getDbsnpIds();

    Map<String, Set<String>> getHgvs();

    Collection<? extends VariantSourceEntry> getSourceEntries();

    IVariantSourceEntry getSourceEntry(String fileId, String studyId);

}
