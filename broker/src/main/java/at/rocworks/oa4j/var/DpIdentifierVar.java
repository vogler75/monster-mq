/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package at.rocworks.oa4j.var;

import java.io.Serializable;
import java.util.regex.Pattern;

/**
 *
 * @author vogler
 */
public class DpIdentifierVar extends Variable implements Serializable {
    String value;
    String[] parts = null; // system : dpel : config
    
    public DpIdentifierVar() {
        this.value="";
    }

    public DpIdentifierVar(String name) {
        this.value=name;
    }

    public DpIdentifierVar(String name, String defaultConfig) {
        this.value=addConfigIfNotExists(name, defaultConfig);
    }
    
    public String getValue() {
        return this.value;
    }
    
    public String getName() {
        return this.value;
    }
    
    public void setName(String name) {
        this.value=name;
        this.parts=null;
    }    
    
    @Override
    public String formatValue() {
        return this.value;
    }

    @Override
    public VariableType isA() {
        return VariableType.DpIdentifierVar;
    }
    
    public boolean isInternal() {
        return getDp().startsWith("_");
    }
    
    @Override
    public Object getValueObject() {
        return value; 
    }

    private void splitName() {
        if (parts==null) {
            String[] arr = Pattern.compile(":").split(value);
            String[] res = {"", "", ""};
            if (arr.length == 1) {
                res[1] = arr[0]; // dpel
            } else if (arr.length == 2) {
                if (arr[0].contains(".")) {
                    res[1] = arr[0]; // dpel
                    res[2] = arr[1]; // _config.._attribute
                } else if (arr[1].contains(".")) {
                    res[0] = arr[0]; // system
                    res[1] = arr[1]; // dpel
                }
            } else if (arr.length == 3) {
                res = arr;
            }
            parts=res;
        }
    }

    public String getSystem() {
        //Pattern p = Pattern.compile(":");
        //String[] s = p.split(this.value);
        //return s[0];
        splitName();
        return parts[0];
    }

    public String getDp() {
        //Pattern p = Pattern.compile(":|\\.");
        //String[] s = p.split(this.value);
        //return s[1];
        splitName();
        return parts[1];
    }
    
    public String getDpEl() {
        //Pattern p = Pattern.compile(":");
        //String[] s = p.split(this.value);
        //return s[1];
        splitName();
        return parts[2];
    }
    
    public String getSysDpEl() {
        //Pattern p = Pattern.compile(":");
        //String[] s = p.split(this.value);
        //return s[0]+":"+s[1];
        splitName();
        return parts[0]+":"+parts[1];
    }    

    public String getElement() {
        //int start = this.value.indexOf('.') + 1;
        //int end = this.value.indexOf(':', start);
        //return (end < 0 ? this.value.substring(start) : this.value.substring(start, end));
        splitName();
        int start = parts[1].indexOf('.') + 1;
        return parts[1].substring(start);
    }

    public String getConfig() {
        //Pattern p = Pattern.compile(":");
        //String[] s = p.split(this.value);
        //return s.length == 3 ? s[2] : "";
        splitName();
        return parts[2];
    }    
    
    @Override
    public int hashCode() {
        return this.value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final DpIdentifierVar other = (DpIdentifierVar) obj;
        return this.value.equals(other.value);
    }
    
    public static DpIdentifierVar newDpIdentifier() {
        return new DpIdentifierVar();
    } 
    
    public static DpIdentifierVar newDpIdentifier(String dp) {
        return new DpIdentifierVar(dp);
    } 
            
    public static DpIdentifierVar[] newDpIdentifier(String[] dps) {
        DpIdentifierVar[] res;
        res = new DpIdentifierVar[dps.length];
        for ( int i=0; i<dps.length; i++)
            res[i]=new DpIdentifierVar(dps[i]);        
        return res;
    }

    public static String addConfigIfNotExists(String dp, String config) {
        return new DpIdentifierVar(dp).getConfig().equals("") ? dp+":"+config : dp;
    }
}
