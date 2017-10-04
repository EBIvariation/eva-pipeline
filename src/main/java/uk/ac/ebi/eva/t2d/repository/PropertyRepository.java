package uk.ac.ebi.eva.t2d.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ebi.eva.t2d.entity.Property;

@Repository
@Transactional
public interface PropertyRepository extends ExpandedCrudRepository<Property, String> {
}
