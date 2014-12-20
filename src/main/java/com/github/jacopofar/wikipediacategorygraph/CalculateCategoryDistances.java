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

import static com.github.jacopofar.wikipediacategorygraph.CreateCategoryGraph.categoryLbl;
import java.util.LinkedList;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

/**
 * Calculate the distances of each category from the chosen ones, with these criteria:
 * 1. a step from a category to a sub-category costs 1
 * 2. a step from a category to a container category costs 3
 * 3. the distance between two categories is the path with the lower cost possible between them
 */
public class CalculateCategoryDistances {
    public static void main(String args[]){
        GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(args[0]);
        
        LinkedList<DistanceLabel> frontier=new LinkedList<>();
        System.out.println("Initializing the macro-categories...");
        //initialize the frontier with the starting categories
        try ( Transaction tx = graphDb.beginTx()){
            for(String categoryName:args[1].split("\\|")){
                ResourceIterator<Node> matcher = graphDb.findNodesByLabelAndProperty(categoryLbl, "name", categoryName).iterator();
                Node cat=matcher.next();
                DistanceLabel dl = new DistanceLabel(cat,categoryName,0);
                dl.mark();
                frontier.add(dl);
                matcher.close();
            }
            tx.success();
        }
        
        //the next generation frontier, in a two step process
        //when the next frontier is empty, the program has finished
        LinkedList<DistanceLabel> nextFrontier=new LinkedList<>();
        int iterations=0;
        while(frontier.size()>0){
            System.out.println("Iteration number "+iterations+", "+frontier.size()+" nodes to be analyzed...");
            long startTime=System.currentTimeMillis();
                    for(DistanceLabel dstart:frontier){
                        try ( Transaction tx = graphDb.beginTx()){
                            //add subcategories
                            for(Node contained:dstart.getContained()){
                                DistanceLabel candidate = dstart.stepOf(contained,1);
                                //if the node has changed (so it's a new node or has been reached with a lower distance) add it to the next frontier
                                if(candidate.mark()){
                                    nextFrontier.add(candidate);
                                }
                            }
                            //add super-categories, the same but with a greater distance score.
                            //paths can go upward the category tree but with a greater distance score
                            for(Node contained:dstart.getContainers()){
                                DistanceLabel candidate = dstart.stepOf(contained,3);
                                //if the node has changed (so it's a new node or has been reached with a lower distance) add it to the next frontier
                                if(candidate.mark()){
                                    nextFrontier.add(candidate);
                                }
                            }
                            tx.success();
                        }
                    }
                    System.out.println("Iteration number "+iterations+" finished in "+(System.currentTimeMillis()-startTime)/1000+"s, "+nextFrontier.size()+" nodes pending for the next iteration");
                    frontier=nextFrontier;
                    nextFrontier=new LinkedList<>();
                    iterations++;
        }
    }
}
