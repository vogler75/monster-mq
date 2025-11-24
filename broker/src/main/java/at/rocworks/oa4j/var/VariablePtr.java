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
public class VariablePtr {
    private Variable var;
    
    public VariablePtr() {
        var=null;
    }
    
    public VariablePtr(Variable var) {
        this.var=var;
    }
    
    public Variable get() {
        return var;
    }
    
    public void set(Variable var) {
        this.var=var;
    }
}
