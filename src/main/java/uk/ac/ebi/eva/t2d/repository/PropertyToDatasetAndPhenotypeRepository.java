package uk.ac.ebi.eva.t2d.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ebi.eva.t2d.entity.Phenotype;
import uk.ac.ebi.eva.t2d.entity.PropertyToDatasetAndPhenotype;
import uk.ac.ebi.eva.t2d.entity.embedded.id.PropertyIdDatasetIdPhenotypeId;
import uk.ac.ebi.eva.t2d.model.T2DTableStructure;

@Repository
@Transactional
public interface PropertyToDatasetAndPhenotypeRepository extends CrudRepository<PropertyToDatasetAndPhenotype,
        PropertyIdDatasetIdPhenotypeId> {

    default void save(String datasetId, T2DTableStructure structure, Phenotype phenotype) {
        save(generate(datasetId, structure, phenotype));
    }

    // TODO promove to private on java 9
    default Iterable<PropertyToDatasetAndPhenotype> generate(String datasetId, T2DTableStructure structure,
                                                             Phenotype phenotype) {
        return structure.getFields().stream()
                .map(field -> new PropertyToDatasetAndPhenotype(field.getKey(), datasetId, phenotype.getId()))
                ::iterator;
    }

}
