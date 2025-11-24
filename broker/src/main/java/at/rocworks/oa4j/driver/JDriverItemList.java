/*
    OA4J - WinCC Open Architecture for Java
    Copyright (C) 2017 Andreas Vogler

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/
package at.rocworks.oa4j.driver;

import java.util.LinkedList;

/**
 *
 * @author vogler
 */
public class JDriverItemList {

    private final LinkedList<JDriverItem> collection;

    public JDriverItemList() {
        collection=new LinkedList<>();
    }

    public JDriverItemList(JDriverItem item) {
        collection=new LinkedList<>();
        collection.add(item);
    }

    public void addItem(JDriverItem item) {
        collection.add(item);
    }

    public JDriverItem pollFirst() {
        return collection.pollFirst(); 
    }
    
    public JDriverItem pollLast() {
        return collection.pollLast(); 
    }    

    public int getSize() {
        return collection.size();
    }

    public boolean isEmpty() {
        return collection.isEmpty();
    }
}    
