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

import at.rocworks.oa4j.var.FloatVar;
import at.rocworks.oa4j.var.Variable;
import at.rocworks.oa4j.var.VariableType;
import at.rocworks.oa4j.base.JDebug;
import java.nio.ByteBuffer;
import java.util.logging.Level;

/**
 *
 * @author vogler
 */
public class JTransFloatVar extends JTransBaseVar {
        
    public JTransFloatVar(String name, int type) {
        super(name, type, VariableType.FloatVar, Double.SIZE);           
    }
    
    public JTransFloatVar(String name, int type, int size) {
        super(name, type, VariableType.FloatVar, size);           
    }    
    
    protected byte[] toPeriph(Double val) { return toPeriphStatic(val); }
    public static byte[] toPeriphStatic(Double val) {
        ByteBuffer buffer = ByteBuffer.allocate(Double.SIZE);
        buffer.putDouble(val);
        return buffer.array();        
    }
    
    protected Double toVal(byte[] data) { return toValStatic(data); }
    public static Double toValStatic(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data); // big-endian by default
        return buffer.getDouble();        
    }

    @Override
    public byte[] toPeriph(int dlen, Variable var, int subix) {
        try {
            if ( var.getFloatVar()== null ) {
                JDebug.out.log(Level.WARNING, "toPeriph: Variable has no {0} value!", new Object[]{getVariableType().toString()});
                return null;
            } else {   
                Double val = var.getFloatVar().getValue();
                return toPeriph(val);
            }
        } catch ( Exception ex) {
            JDebug.StackTrace(Level.SEVERE, ex);
            return null;
        }
    }
    
    @Override
    public Variable toVar(byte[] data, int dlen, int subix) {
        try {
            Double val = toVal(data);
            FloatVar var = new FloatVar(val);
            return var;
        } catch ( Exception ex) {
            JDebug.StackTrace(Level.SEVERE, ex);
            return null;
        }        
    }    
}
