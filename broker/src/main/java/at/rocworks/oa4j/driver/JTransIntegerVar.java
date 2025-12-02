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

import at.rocworks.oa4j.base.JManager;
import at.rocworks.oa4j.jni.ErrCode;
import at.rocworks.oa4j.jni.ErrPrio;
import at.rocworks.oa4j.var.IntegerVar;
import at.rocworks.oa4j.var.Variable;
import at.rocworks.oa4j.var.VariableType;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.logging.Level;

/**
 *
 * @author vogler
 */
public class JTransIntegerVar extends JTransBaseVar {            
        
    public JTransIntegerVar(String name, int type) {
        super(name, type, VariableType.IntegerVar, Integer.SIZE);
    }               
    
    public JTransIntegerVar(String name, int type, int size) {
        super(name, type, VariableType.IntegerVar, size);           
    }       

    protected byte[] toPeriph(Integer val) { return toPeriphStatic(val); }
    public static byte[] toPeriphStatic(Integer val) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE);
        buffer.putInt(val);
        return buffer.array();        
    }
    
    protected Integer toVal(byte[] data) { return toValStatic(data); }
    public static Integer toValStatic(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data); // big-endian by default
        return buffer.getInt();        
    }    
    
    @Override
    public byte[] toPeriph(int dlen, Variable var, int subix) {
        try {
            if ( var.getIntegerVar() == null ) {
                JManager.log(ErrPrio.PRIO_WARNING, ErrCode.UNEXPECTEDSTATE, "toPeriph: Variable has no "+getVariableType()+" value!");
                return null;
            } else {
                Integer val = var.getIntegerVar().getValue();
                return toPeriph(val);
            }
        } catch ( Exception ex) {
            JManager.stackTrace(ex);
            return null;
        }
    }   
        
    @Override
    public Variable toVar(byte[] data, int dlen, int subix) {       
        try {
            Integer val = toVal(data);
            IntegerVar var = new IntegerVar(val);
            return var;
        } catch ( Exception ex) {
            JManager.stackTrace(ex);
            return null;
        }        
    }
}
