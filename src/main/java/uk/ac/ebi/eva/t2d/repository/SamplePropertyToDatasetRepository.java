package uk.ac.ebi.eva.t2d.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ebi.eva.t2d.entity.SamplesDatasetMetadata;
import uk.ac.ebi.eva.t2d.entity.SamplePropertyToDataset;
import uk.ac.ebi.eva.t2d.model.T2DTableStructure;

@Repository
@Transactional
public interface SamplePropertyToDatasetRepository extends CrudRepository<SamplePropertyToDataset, String> {

    default void save(String datasetId, T2DTableStructure structure) {
        save(generate(datasetId, structure));
    }

    // TODO promove to private on java 9
    default Iterable<SamplePropertyToDataset> generate(String datasetId, T2DTableStructure structure) {
        return structure.getFields().stream()
                .map(field -> new SamplePropertyToDataset(datasetId, field.getKey()))
                ::iterator;
    }

}
