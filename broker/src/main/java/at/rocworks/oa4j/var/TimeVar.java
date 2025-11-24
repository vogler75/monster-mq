/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package at.rocworks.oa4j.var;

import java.util.Date;
import java.util.TimeZone;

/**
 *
 * @author vogler
 */
public class TimeVar extends Variable {
    public static final String FMT_DATE_JS_SEC = "yyyy-MM-dd'T'HH:mm:ss'Z'";        
    public static final String FMT_DATE_JS_MS = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static final String FMT_DATE_JS_NS = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"; // Nanoseconds   
    
    public static final String FMT_DATE_OA_MS = "yyyy.MM.dd HH:mm:ss.SSS";        
    
    private static TimeZone tz = TimeZone.getDefault();
    
    private Date value;
    
    public TimeVar() {
        this.value = new Date();
    }
    public TimeVar(Date value) {
        this.value = value;
    }
    
    public TimeVar(long ms) { // ms
        this.value = new Date(ms);
    }  
    
    public void setValue(Date value) {
        this.value=value;
    }

    public void setValue(long ms) {
        this.value=new Date(ms);
    }
    
    public Date getValue() {
        return this.value;
    }
    
    public long getTime() {
        return this.value.getTime();
    }    
    
    @Override
    public String formatValue() {
        return value.toString();
    }

    @Override
    public VariableType isA() {
        return VariableType.TimeVar;
    }        
    
    @Override
    public Object getValueObject() {
        return value; 
    }       
    
    public TimeVar getLocalTime() {
        //JDebug.out.info("time="+this.toString()+" offset="+tz.getRawOffset()+" savings="+tz.getDSTSavings()+ " in="+tz.inDaylightTime(this.value) );
        return new TimeVar(new Date(this.getTime() + tz.getRawOffset() + (tz.inDaylightTime(this.value) ? tz.getDSTSavings() :0)));        
    }
    
    public TimeVar getUTCTime() {
        //JDebug.out.info("time="+this.toString()+" offset="+tz.getRawOffset()+" savings="+tz.getDSTSavings()+ " in="+tz.inDaylightTime(this.value) );
        return new TimeVar(new Date(this.getTime() - tz.getRawOffset() - (tz.inDaylightTime(this.value) ? tz.getDSTSavings() :0)));        
    }
}
