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
package at.rocworks.oa4j.jni;

import at.rocworks.oa4j.var.Variable;

/**
 *
 * @author vogler
 */
public abstract class Transformation {
    private final String name;
    private final int type;
    
    public Transformation(String name, int type) {
        this.name=name;
        this.type=type;
    }
    
    public String getName() {
        return name;
    }
    
    public int getType() {
        return type;
    }
       
    public abstract int itemSize();            
    public abstract int getVariableTypeAsInt();

    // Conversion from PVSS to Hardware
    public abstract byte[] toPeriph(int blen, Variable var, int subix);

    // Conversion from Hardware to PVSS
    public abstract Variable toVar(byte[] data, int dlen, int subix);
    
    public abstract void delete();
}
