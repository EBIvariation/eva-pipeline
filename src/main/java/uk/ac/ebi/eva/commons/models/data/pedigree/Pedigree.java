/*
 * Copyright 2014-2016 EMBL - European Bioinformatics Institute
 * Copyright 2015 OpenCB
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
package uk.ac.ebi.eva.commons.models.data.pedigree;

import java.util.*;

public class Pedigree {

    private Map<String, Individual> individuals;
    private Map<String, Set<Individual>> families;
    private Map<String, Integer> fields;

    public Pedigree() {
        individuals = new LinkedHashMap<>(100);
        families = new LinkedHashMap<>(100);
        fields = new LinkedHashMap<>(5);
    }

    public Set<Individual> getFamily(String familyId) {
        return families.get(familyId);
    }

    public Individual getIndividual(String id) {
        return individuals.get(id);
    }

    public Map<String, Individual> getIndividuals() {
        return individuals;
    }

    public void setIndividuals(Map<String, Individual> individuals) {
        this.individuals = individuals;
    }

    public Map<String, Set<Individual>> getFamilies() {
        return families;
    }

    public void setFamilies(Map<String, Set<Individual>> families) {
        this.families = families;
    }

    public Map<String, Integer> getFields() {
        return fields;
    }

    public void setFields(Map<String, Integer> fields) {
        this.fields = fields;
    }

    public void addIndividual(Individual ind) {
        this.individuals.put(ind.getId(), ind);
    }

    public void addIndividualToFamily(String familyId, Individual ind) {
        this.families.get(familyId).add(ind);
    }

    public void addFamily(String familyId, Set<Individual> family) {
        this.getFamilies().put(familyId, family);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Pedigree\n");
        if (fields.size() > 0) {
            sb.append("fields = " + fields.keySet().toString() + "\n");
        }

        for (Map.Entry<String, Set<Individual>> elem : this.families.entrySet()) {
            sb.append(elem.getKey() + "\n");
            for (Individual ind : elem.getValue()) {
                sb.append("\t" + ind.toString() + "\n");
            }
        }
        return sb.toString();
    }

    public List<Family> getFamiliesTDT() {

        List<Family> families = new ArrayList<>();
        individuals.forEach((s, individual) -> {
            if (individual.getFather() != null && individual.getMother() != null) {
                addIndividualToFamily(individual, individual.getFather(), individual.getMother(), families);
            }
        });
        return families;
    }

    private static void addIndividualToFamily(Individual individual, Individual father, Individual mother,
                                              List<Family> families) {
        for (Family family : families) {
            if (family.getFather().equals(father) && family.getMother().equals(mother)) {
                family.addChild(individual);
                return;
            }
        }
        Family family = new Family(father, mother);
        family.addChild(individual);
        families.add(family);
    }

}
