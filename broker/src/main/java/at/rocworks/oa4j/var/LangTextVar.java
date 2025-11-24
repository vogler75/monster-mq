/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package at.rocworks.oa4j.var;

import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author vogler
 */
public class LangTextVar extends Variable {

    private HashMap<String, String> value;
    
    public LangTextVar() {
        this.value = new HashMap<>();
    }
    
    public void setValue(int langId, String text) {
        this.value.put(Integer.toString(langId), text);
    }
    
    public String getValue(int langId) {
        return this.value.get(Integer.toString(langId));
    }
    
    @Override
    public String formatValue() {
        ArrayList<String> arr = new ArrayList<>(value.size());
        value.forEach((String lang, String text)->arr.add(lang+"->"+text));
        return "["+String.join(",", arr)+"]";
    }

    @Override
    public VariableType isA() {
        return VariableType.LangTextVar;
    }
    
    @Override
    public Object getValueObject() {
        return value; 
    }            
}
