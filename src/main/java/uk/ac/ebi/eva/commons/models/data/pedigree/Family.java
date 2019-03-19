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

import java.util.Set;
import java.util.TreeSet;

public class Family {

    private final Individual father;
    private final Individual mother;
    private final Set<Individual> children;

    Family() {
        //Spring empty constructor
        this(null, null);
    }

    public Family(Individual father, Individual mother) {
        this.father = father;
        this.mother = mother;
        this.children = new TreeSet<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Family)) return false;

        Family family = (Family) o;

        return this.father.getId().equals(family.getFather().getId()) &&
                this.mother.getId().equals(family.getMother().getId());
    }

    @Override
    public int hashCode() {
        int result = father != null ? father.hashCode() : 0;
        result = 31 * result + (mother != null ? mother.hashCode() : 0);
        result = 31 * result + (children != null ? children.hashCode() : 0);
        return result;
    }

    public Individual getFather() {
        return father;
    }

    public Individual getMother() {
        return mother;
    }

    public void addChild(Individual ind) {
        this.children.add(ind);
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();

        sb.append("{");
        sb.append("father=");
        sb.append(father.getId());
        sb.append(", mother=");
        sb.append(mother.getId());

        if (children.size() > 0) {
            sb.append(", children=[");
            for (Individual ind : children) {
                sb.append(ind.getId() + " ");
            }
            sb.append("]");
        }
        sb.append("}\n");
        return sb.toString();
    }

}
