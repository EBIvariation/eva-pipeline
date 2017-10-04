package uk.ac.ebi.eva.t2d.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.RepositoryDefinition;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ebi.eva.t2d.entity.SampleProperty;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Repository
@Transactional
public interface SamplePropertyRepository extends ExpandedCrudRepository<SampleProperty, String> {

}
