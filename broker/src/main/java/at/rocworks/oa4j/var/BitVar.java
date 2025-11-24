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
public class BitVar extends Variable {
    
    private Boolean value;
    
    public BitVar(Boolean value) {
        this.value = value;
    }
    
    public BitVar(boolean value) {
        this.value = value;
    }
    
    public BitVar(int value) {
        this.value = (value != 0);
    }    
    
    public void setValue(Boolean value) {
        this.value=value;
    }
    
    public Boolean getValue() {
        return this.value;
    }
    
    @Override
    public String formatValue() {
        return value.toString();
    }

    @Override
    public VariableType isA() {
        return VariableType.BitVar;
    }            

    @Override
    public Object getValueObject() {
        return value; 
    }
}
