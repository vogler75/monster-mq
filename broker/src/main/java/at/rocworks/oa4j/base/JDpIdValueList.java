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
package at.rocworks.oa4j.base;

import at.rocworks.oa4j.var.DpIdentifierVar;
import at.rocworks.oa4j.var.NullVar;
import at.rocworks.oa4j.var.Variable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author vogler
 */
public class JDpIdValueList implements Iterable<JDpVCItem> {
    private final ArrayList<JDpVCItem> list = new ArrayList<>();
    
    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append(this.getClass().getSimpleName()).append(":").append(list.size()).append("\n");
        list.forEach((JDpVCItem item)->s.append(item.toString()).append("\n"));
        return s.toString();
    }
    
    public void addItem(JDpVCItem item) {
        this.list.add(item);
    }
    
    public JDpVCItem getItem(int idx) {
        return list.get(idx);
    }

    public Variable getItemVar(int idx) {
        JDpVCItem item = list.size()>idx ? list.get(idx) : null;
        return item==null ? NullVar.NULL : item.getVariable();
    }
    
    public ArrayList<JDpVCItem> getItems() {
        return list;
    }
    
    public int getNumberOfItems() { // wincc oa api like
        return list.size();
    } // WinCC OA API Style

    public int size() { return list.size(); } // Java Style
    
    public List<JDpVCItem> asList() {
        return list;
    }
        
    public void clear() {
        list.clear();
    }

    /**
     * Returns an iterator over elements of type {@code T}.
     *
     * @return an Iterator.
     */
    @Override
    public Iterator<JDpVCItem> iterator() {
        return asList().iterator();
    }
}
