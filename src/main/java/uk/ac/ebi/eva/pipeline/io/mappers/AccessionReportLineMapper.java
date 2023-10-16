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
package uk.ac.ebi.eva.pipeline.io.mappers;

import org.springframework.batch.item.file.LineMapper;
import uk.ac.ebi.eva.commons.core.models.VariantCoreFields;
import uk.ac.ebi.eva.commons.models.data.Variant;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class AccessionReportLineMapper extends VariantVcfFactory implements LineMapper<Variant> {
    public AccessionReportLineMapper() {
    }

    @Override
    public Variant mapLine(String line, int lineNumber) {
        String[] fields = line.split("\t");
        if (fields.length < 5) {
            throw new IllegalArgumentException("Not enough fields provided (min 5)");
        }

        String chromosome = fields[0];
        int position = Integer.parseInt(fields[1]);
        String reference = getReference(fields);
        String alternateAllele = Objects.nonNull(fields[4]) ? fields[4].toUpperCase() : null ;

        VariantCoreFields keyFields = getVariantCoreKeyFields(chromosome, position, reference, alternateAllele);
        Variant variant = new Variant(chromosome, (int) keyFields.getStart(), (int) keyFields.getEnd(), keyFields.getReference(), keyFields.getAlternate());

        variant.setIds(getIds(fields));

        return variant;
    }

    private String getReference(String[] fields) {
        return fields[3].equals(".") ? "" : fields[3].toUpperCase();
    }

    private Set<String> getIds(String[] fields) {
        Set<String> ids = new HashSet<>();
        if (!fields[2].equals(".")) {
            ids.addAll(Arrays.asList(fields[2].split(";")));
        }
        return ids;
    }

    /**
     * @param chromosome
     * @param position
     * @param reference
     * @param alternateAllele
     * @return VariantCoreFields
     * When reading variant from Accessioned VCF, this method checks if a context base has been added to the Variant.
     * If yes, we need to remove that first, in order to make the representation consistent and then give to VariantCoreFields
     * for other checks
     *
     * ex: Assume the following variant   ->     After right trimming    ->     stored in vcf
     * CHR POS  REF  ALT                         CHR POS REF ALT                CHR POS REF  ALT
     * 1   100  CAGT  T                          1  100 CAG                     1  99  GCAG  G
     *
     * Storing in VCF (as per normalition algorithm, VCF cannot store an empty REF or ALT. If after right trimming REF or ALT become empty,
     * a context base needs to be added)
     *
     * reading without context base adjustment (erroneous)                  reading with context base adjustment
     * CHR POS REF ALT                                                      CHR POS REF ALT
     * 1   99  GCA                                                          1   100 CAG
     */
    private VariantCoreFields getVariantCoreKeyFields(String chromosome, long position, String reference, String alternateAllele) {
        if (isContextBasePresent(reference, alternateAllele)) {
            if (alternateAllele.length() == 1) {
                alternateAllele = "";
                reference = reference.substring(1);
            } else if (reference.length() == 1) {
                reference = "";
                alternateAllele = alternateAllele.substring(1);
            }
            position = position + 1;
        }
        return new VariantCoreFields(chromosome, position, reference, alternateAllele);
    }

    private boolean isContextBasePresent(String reference, String alternate) {
        if (alternate.length() == 1 && reference.length() > 1 && reference.startsWith(alternate)) {
            return true;
        } else if (reference.length() == 1 && alternate.length() > 1 && alternate.startsWith(reference)) {
            return true;
        } else {
            return false;
        }
    }
}
