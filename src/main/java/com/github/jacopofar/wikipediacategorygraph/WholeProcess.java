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

import java.io.IOException;

/**
 * Run the three steps of the process:
 * 1. load the category nodes (ID and name) from the category.sql file
 * 2. load edge data from categorylinks.sql and create article nodes
 * 3. calculate category distances from the given set of starting categories
 * 
 * The process is described in the paper "Automatically assigning Wikipedia articles to macro-categories"
 */
public class WholeProcess {
    public static void main(String argc[]) throws IOException{
        String categoryFile="category.sql";
        String categoryLinksFile="categorylinks.sql";
        String dbFolder="categorygraph";
        if(argc.length==3){
            categoryFile=argc[0];
            categoryLinksFile=argc[1];
            dbFolder=argc[2];
        }
        CreateCategoryGraph.main(new String[]{categoryFile,categoryLinksFile,dbFolder});
        CalculateCategoryDistances.main(new String[]{dbFolder,"History|Geography|People"});
    }
}
