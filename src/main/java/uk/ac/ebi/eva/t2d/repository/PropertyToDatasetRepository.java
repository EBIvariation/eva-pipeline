package uk.ac.ebi.eva.t2d.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ebi.eva.t2d.entity.PropertyToDataset;
import uk.ac.ebi.eva.t2d.entity.embedded.id.PropertyIdDatasetId;
import uk.ac.ebi.eva.t2d.model.T2DTableStructure;

@Repository
@Transactional
public interface PropertyToDatasetRepository extends CrudRepository<PropertyToDataset, PropertyIdDatasetId> {

    default void save(String datasetId, T2DTableStructure structure) {
        save(generate(datasetId, structure));
    }

    // TODO promove to private on java 9
    default Iterable<PropertyToDataset> generate(String datasetId, T2DTableStructure structure) {
        return structure.getOrderedFieldIdSet().stream()
                .map(field -> new PropertyToDataset(field, datasetId))
                ::iterator;
    }

}
