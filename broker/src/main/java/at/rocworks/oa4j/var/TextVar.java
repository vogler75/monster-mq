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
public class TextVar extends Variable {

    private String value;
    
    public TextVar(String value) {
        this.value = value;
    }
    
    public void setValue(String value) {
        this.value=value;
    }
    
    public String getValue() {
        return this.value;
    }
    
    @Override
    public String formatValue() {
        return value;
    }

    @Override
    public VariableType isA() {
        return VariableType.TextVar;
    }
    
    @Override
    public Object getValueObject() {
        return value; 
    }
}
