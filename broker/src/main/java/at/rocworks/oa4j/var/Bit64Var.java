/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package at.rocworks.oa4j.var;

/**
 *
 * @author vogler
 */
public class Bit64Var extends Bit32Var {
    public Bit64Var(Long value) {
        super(value);
    }
    
    public Bit64Var(int value) {
        super(value);
    }  
    
    public Bit64Var(long value) {
        super(value);
    }      
    
    public Bit64Var(Object value) {
        super(value);
    }    
    
    @Override
    public VariableType isA() {
        return VariableType.Bit64Var;
    }      
}
