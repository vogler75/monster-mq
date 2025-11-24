/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package at.rocworks.oa4j.var;

import java.math.BigDecimal;

/**
 *
 * @author vogler
 */
public class Bit32Var extends Variable {
    private Long value;
    
    public Bit32Var(Long value) {
        this.value = value;
    }
    
    public Bit32Var(int value) {
        this.value = (long)value;
    }  
    
    public Bit32Var(long value) {
        this.value = (long)value;
    }      
    
    public Bit32Var(Object value) {
        if ( value instanceof java.lang.Integer )
            this.value = ((Integer) value).longValue();
        else if ( value instanceof java.lang.Long )
            this.value = ((Long) value);   
        else if ( value instanceof java.math.BigDecimal )
            this.value = ((BigDecimal) value).longValue();            
        else
            throw new UnsupportedOperationException("Type "+value.getClass().getName()+" ["+value.toString()+"] not supported yet."); //To change body of generated methods, choose Tools | Templates.                    
    }
    
    public void setValue(Integer value) {
        this.value=value.longValue();
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
        return VariableType.Bit32Var;
    }        
    
    @Override
    public Object getValueObject() {
        return value; 
    }        
}
