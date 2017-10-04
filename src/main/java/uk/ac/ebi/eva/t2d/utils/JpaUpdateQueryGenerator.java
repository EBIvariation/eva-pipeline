/*
 * Copyright 2016-2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.t2d.utils;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.Map;

/**
 * This class serves to create and prepare a jpa sql query. This class needs to be extended for each different query
 * that can be created.
 *
 * @param <T>
 */
public abstract class JpaUpdateQueryGenerator<T> {

    private final boolean nativeQuery;

    public JpaUpdateQueryGenerator(boolean nativeQuery) {
        this.nativeQuery = nativeQuery;
    }

    public void executeQuery(EntityManager entityManager, T entity) {
        Query jpaQuery = generateQuery(entityManager, entity);
        jpaQuery.executeUpdate();
    }

    Query generateQuery(EntityManager entityManager, T entity) {
        Query jpaQuery = null;
        String statement = prepareStatement(entity);
        if (nativeQuery) {
            jpaQuery = entityManager.createNativeQuery(statement);
        } else {
            jpaQuery = entityManager.createQuery(statement);
        }
        Map<String, String> parameters = prepareParameters(entity);
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            jpaQuery.setParameter(entry.getKey(), entry.getValue());
        }
        return jpaQuery;
    }

    protected abstract Map<String, String> prepareParameters(T entity);

    protected abstract String prepareStatement(T entity);
}
