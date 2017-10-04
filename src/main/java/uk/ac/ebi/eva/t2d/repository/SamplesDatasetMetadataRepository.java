package uk.ac.ebi.eva.t2d.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ebi.eva.t2d.entity.SamplesDatasetMetadata;

@Repository
@Transactional
public interface SamplesDatasetMetadataRepository extends CrudRepository<SamplesDatasetMetadata, String> {


}
