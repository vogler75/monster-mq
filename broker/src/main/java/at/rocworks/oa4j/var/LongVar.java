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
public class LongVar extends Variable {
    
    private Long value;
    
    public LongVar(Long value) {
        this.value = value;
    }
    
    public LongVar(long value) {
        this.value = value;
    }  
    
    public LongVar(int value) {
        this.value = (long)value;
    }      
    
    public void setValue(Long value) {
        this.value=value;
    }
    
    public Long getValue() {
        return this.value;
    }    

    @Override
    public String formatValue() {
        return value.toString();
    }

    @Override
    public VariableType isA() {
        return VariableType.LongVar;
    }

    @Override
    public Object getValueObject() {
        return value;
    }    
}
