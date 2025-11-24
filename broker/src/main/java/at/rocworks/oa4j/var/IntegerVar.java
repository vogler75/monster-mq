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
public class IntegerVar extends Variable {
    
    private Integer value;
    
    public IntegerVar(Integer value) {
        this.value = value;
    }
    
    public IntegerVar(int value) {
        this.value = value;
    }  
    
    public IntegerVar(long value) {
        this.value = (int)value;
    }      
    
    public void setValue(Integer value) {
        this.value=value;
    }
    
    public Integer getValue() {
        return this.value;
    }
    
    @Override
    public String formatValue() {
        return value.toString();
    }

    @Override
    public VariableType isA() {
        return VariableType.IntegerVar;
    }        
    
    @Override
    public Object getValueObject() {
        return value; 
    }    
}
