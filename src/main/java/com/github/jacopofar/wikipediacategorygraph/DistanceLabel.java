/* 
 * Copyright 2014 Jacopo farina.
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
package com.github.jacopofar.wikipediacategorygraph;

import java.util.LinkedList;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 *
 * Represents a distance label associated to a node.
 * That is, just a String with the category name and a distance value, and the corresponding Node
 */
public class DistanceLabel {
    private final String category;
    private final int distance;
    private final Node node;
    public DistanceLabel(Node cat,String category,int distance){
        this.category=category;
        this.distance=distance;
        this.node=cat;
    }
    
    
    /**
     * @return the category
     */
    public String getCategory() {
        return category;
    }
    
    /**
     * @return the distance
     */
    public int getDistance() {
        return distance;
    }
    public String toString(){
        return "["+category+"] "+distance;
    }
    
    /**
     * Mark the given node with this distance,  assigning it the corresponding property if not present
     * Returns true if the property wasn't present or was present with a greater distance, false otherwise
     */
    boolean mark() {
        int dist = (int) node.getProperty("distance_"+category, Integer.MAX_VALUE);
        if(dist>distance){
            node.setProperty("distance_"+category, distance);
            return true;
        }
        return false;
    }
    /**
     * Return the categories contained in the marked one
     */
    Iterable<Node> getContained() {
        LinkedList<Node> ret=new LinkedList<>();
        for(Relationship r:node.getRelationships(Direction.INCOMING, CreateCategoryGraph.subCategoryOfRel)){
            ret.add(r.getStartNode());
        }
        return ret;
    }
    
    /**
     * Return a Distance Label with the same label, the given node and a distance increased of i
     */
    DistanceLabel stepOf(Node contained, int i) {
        return new DistanceLabel(contained,category,distance+i);
    }
    /**
     * Return the categories containing the marked one
     */
    Iterable<Node> getContainers() {
        LinkedList<Node> ret=new LinkedList<>();
        for(Relationship r:node.getRelationships(Direction.OUTGOING, CreateCategoryGraph.subCategoryOfRel)){
            ret.add(r.getEndNode());
        }
        return ret;    }
}
