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

import at.rocworks.oa4j.jni.Transformation;
import at.rocworks.oa4j.var.VariableType;

/**
 *
 * @author vogler
 */
public abstract class JTransBaseVar extends Transformation {
    private final int itemSize;
    private final VariableType varType;
    
    public JTransBaseVar(String name, int type, VariableType varType, int itemSize) {
        super(name, type);
        this.varType = varType;
        this.itemSize = itemSize;
    }       

    @Override
    public int itemSize() {
        return itemSize;
    }        

    @Override
    public int getVariableTypeAsInt() {
        return varType.value;
    }    
    
    public VariableType getVariableType() {
        return varType;
    }
    
    @Override
    public void delete() {
        //JDebug.out.log(Level.INFO, "delete {0}", getName());
    }
}
