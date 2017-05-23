package uk.ac.ebi.eva.commons.models.mongo.entity.projections;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;
import uk.ac.ebi.eva.commons.models.data.Variant;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.HgvsMongo;
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantAt;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.ALTERNATE_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.AT_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.CHROMOSOME_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.END_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.HGVS_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.LENGTH_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.REFERENCE_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.START_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.TYPE_FIELD;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.buildVariantId;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.createHgvsMongo;
import static uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument.generateAtField;

public class SimplifiedVariant {

    @Id
    private String id;

    @Field(TYPE_FIELD)
    private Variant.VariantType variantType;

    @Field(CHROMOSOME_FIELD)
    private String chromosome;

    @Field(START_FIELD)
    private int start;

    @Field(END_FIELD)
    private int end;

    @Field(LENGTH_FIELD)
    private int length;

    @Field(REFERENCE_FIELD)
    private String reference;

    @Field(ALTERNATE_FIELD)
    private String alternate;

    @Field(AT_FIELD)
    private VariantAt at;

    @Field(HGVS_FIELD)
    private Set<HgvsMongo> hgvs;

    public SimplifiedVariant(Variant.VariantType variantType, String chromosome, int start, int end, int length,
                             String reference, String alternate, Map<String, Set<String>> hgvs) {
        this.id = buildVariantId(chromosome, start, reference, alternate);
        this.variantType = variantType;
        this.chromosome = chromosome;
        this.start = start;
        this.end = end;
        this.length = length;
        this.reference = reference;
        this.alternate = alternate;
        this.at = generateAtField(chromosome, start);
        this.hgvs = createHgvsMongo(hgvs);
    }

}
