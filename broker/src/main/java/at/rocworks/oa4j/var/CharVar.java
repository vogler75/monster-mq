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
public class CharVar extends Variable {
    
    private Character value;
    
    public CharVar(Character value) {
        this.value = value;
    }
    
    public CharVar(char value) {
        this.value = value;
    }
    
    public void setValue(Character value) {
        this.value=value;
    }
    
    public Character getValue() {
        return this.value;
    }
    
    @Override
    public String formatValue() {
        return value.toString();
    }

    @Override
    public VariableType isA() {
        return VariableType.CharVar;
    }            
 
    @Override
    public Object getValueObject() {
        return value; 
    }    
}
