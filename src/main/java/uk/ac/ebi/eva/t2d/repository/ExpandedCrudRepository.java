/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.t2d.repository;

import org.springframework.data.repository.CrudRepository;
import uk.ac.ebi.eva.t2d.entity.EntityWithId;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public interface ExpandedCrudRepository<ENTITY extends EntityWithId<IDTYPE>, IDTYPE extends Serializable>
        extends CrudRepository<ENTITY, IDTYPE> {

    default void insertIfNotExists(ENTITY property) {
        if (findOne(property.getId()) == null) {
            save(property);
        }
    }

    default void insertIfNotExists(List<ENTITY> properties) {
        Set<IDTYPE> propertyIds = properties.stream().map(ENTITY::getId).collect(Collectors.toSet());
        Set<IDTYPE> existingIds = getExistingPropertyIds(propertyIds);
        save(filterPropertiesById(properties, existingIds));
    }

    /**
     * Given a series of Ids filter the ones that exist in the database.
     *
     * @param propertyIds
     * @return
     */
    default Set<IDTYPE> getExistingPropertyIds(Iterable<IDTYPE> propertyIds) {
        return StreamSupport.stream(findAll(propertyIds).spliterator(), false)
                .map(property -> property.getId())
                .collect(Collectors.toSet());
    }

    /**
     * Filter a collection of properties which id exists in the id collection.
     * <p>
     * TODO Promote to private method on java 9
     *
     * @param properties
     * @param ids
     * @return
     */
    default Iterable<ENTITY> filterPropertiesById(Collection<ENTITY> properties, Set<IDTYPE> ids) {
        return properties.stream().filter(property -> !ids.contains(property.getId()))::iterator;
    }

}
