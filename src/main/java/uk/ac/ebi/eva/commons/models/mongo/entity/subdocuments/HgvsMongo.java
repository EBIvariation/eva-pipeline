package uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments;

import org.springframework.data.mongodb.core.mapping.Field;

public class HgvsMongo {

    private static final String TYPE_FIELD = "type";

    private static final String NAME_FIELD = "name";

    @Field(TYPE_FIELD)
    private final String type;

    @Field(NAME_FIELD)
    private final String name;

    public HgvsMongo(String type, String name) {
        this.type = type;
        this.name = name;
    }
}
