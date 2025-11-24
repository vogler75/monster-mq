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
public class FloatVar extends Variable {
    private Double value;
    
    public FloatVar(Double value) {
        this.value = value;
    }
    
    public FloatVar(double value) {
        this.value = value;
    }
    
    public FloatVar(float value) {
        this.value = (double)value;
    }        
    
    public void setValue(Double value) {
        this.value=value;
    }
    
    public Double getValue() {
        return this.value;
    }
    
    @Override
    public String formatValue() {
        return value.toString();
    }

    @Override
    public VariableType isA() {
        return VariableType.FloatVar;
    }    
    
    @Override
    public Object getValueObject() {
        return value; 
    }    
}
