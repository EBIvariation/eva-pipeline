/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.t2d.utils;


import org.opencb.biodata.models.variant.VariantSource;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntry;

import java.util.HashMap;
import java.util.Map;

import static uk.ac.ebi.eva.t2d.utils.VariantUtils.getVariantId;

public class T2dInsertStatisticsQueryGenerator extends JpaUpdateQueryGenerator<Variant> {

    private final VariantSource variantSource;

    public T2dInsertStatisticsQueryGenerator(VariantSource variantSource) {
        super(true);
        this.variantSource = variantSource;
    }

    @Override
    protected String prepareStatement(Variant entity) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("INSERT INTO ").append(variantSource.getStudyId()).append(" (VAR_ID");

        VariantSourceEntry entry = entity.getSourceEntry(variantSource.getFileId(), variantSource.getStudyId());
        for (String attribute : entry.getAttributes().keySet()) {
            stringBuilder.append(", ").append(attribute);
        }
        stringBuilder.append(") VALUES(:VAR_ID");
        for (String attribute : entry.getAttributes().keySet()) {
            stringBuilder.append(", :").append(attribute);
        }
        stringBuilder.append(");");
        return stringBuilder.toString();
    }

    @Override
    protected Map<String, String> prepareParameters(Variant entity) {
        HashMap<String, String> queryParameters = new HashMap<>();
        queryParameters.put("VAR_ID", getVariantId(entity));
        queryParameters.putAll(entity.getSourceEntry(variantSource.getFileId(), variantSource.getStudyId()).getAttributes());
        return queryParameters;
    }

}
