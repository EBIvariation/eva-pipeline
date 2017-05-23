package uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments;

import org.springframework.data.mongodb.core.mapping.Field;

import java.util.HashSet;
import java.util.Set;

public class VariantAt {

    private static final String CHUNK_IDS_FIELD = "chunkIds";

    @Field(CHUNK_IDS_FIELD)
    private Set<String> chunkIds;

    VariantAt(){
        //Empty constructor for spring
    }

    public VariantAt(String chunkSmall, String chunkBig) {
        chunkIds = new HashSet<>();
        chunkIds.add(chunkSmall);
        chunkIds.add(chunkBig);
    }
}
