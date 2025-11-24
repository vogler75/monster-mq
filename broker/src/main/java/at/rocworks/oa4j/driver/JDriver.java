/*
    OA4J - WinCC Open Architecture for Java
    Copyright (C) 2017 Andreas Vogler

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/
package at.rocworks.oa4j.driver;

import at.rocworks.oa4j.base.JDpAttrAddrDirection;
import at.rocworks.oa4j.jni.Driver;
import at.rocworks.oa4j.base.JDebug;
import at.rocworks.oa4j.var.DpIdentifierVar;
import at.rocworks.oa4j.var.Variable;

import java.io.IOException;
import java.util.logging.Level;

/**
 *
 * @author vogler
 */
public abstract class JDriver extends Driver implements Runnable {
    private static JDriver instance = null; // Singleton
        
    private String args[];    
           
    private volatile boolean apiEnabled = false; 
    
    private String projName="<unknown>";
    private String projDir=".";        
    private String confDir=".";
    private int manNum=1;

    public boolean isOutputDisabled() {
        return outputDisabled;
    }

    private volatile boolean outputDisabled = false;
        
    public JDriver(String args[]) throws Exception {         
        initArgs(args);
        initDriver();
    }
        
    public static JDriver getInstance() {
        return JDriver.instance;
    }    
    
    public String getProjPath() { return projDir; }
    private JDriver setProjPath(String projDir) { 
        this.projDir=projDir; 
        this.confDir=this.projDir+"/config";                 
        return this; 
    }        
    
    public JDriver setProjName(String projName) {
        this.projName=projName;
        return this;
    }
    
    public String getConfigDir() { return confDir; }
    public String getLogDir() { return apiGetLogPath(); }
    public String getConfigValue(String key) { return apiGetConfigValue(key); }
    public String getConfigValueOrDefault(String key, String def) {
        String val = apiGetConfigValue(key);
        return (val==null || val.isEmpty()) ? def : val;
    }
    
    public boolean isEnabled() { return apiEnabled; }    
    
    public int getManNum() { return manNum; }
    public JDriver setManNum(int manNum) { this.manNum=manNum; return this; }    
    
    public String[] getArgs() { return args; }
    private void initArgs(String args[]) throws Exception {   
        this.args=args;
        for ( int i=0; i<args.length; i++ ) { 
            // projDir & configDir
            if ( args[i].equals("-path") && args.length>i+1 ) {
                setProjPath(args[i+1]);        
            }
            
            if ( args[i].equals("-proj") && args.length>i+1 ) {
                setProjName(args[i+1]);
            }            
                        
            // managerNum
            if ( args[i].equals("-num") && args.length>i+1 ) {
                setManNum(Integer.valueOf(args[i+1]));
            }                    
        }        
    }    
    
    private void initDriver() throws Exception {
        if (JDriver.instance == null) {
            JDriver.instance = this;
        } else {
            throw new Exception("There can only be one driver!");
        }   
                
        apiEnabled=false;        
        String errmsg="";        
        try {   
            System.loadLibrary("WCCOAjavadrv");
            apiEnabled=true;
        } catch ( java.lang.UnsatisfiedLinkError ex ) {            
            errmsg=ex.getMessage();
        }

        // Set log file settings
        JDebug.setOutput(getLogDir(), getManName());

        if ( !apiEnabled ) {
            JDebug.out.warning(errmsg);
        } 
    }    
            
    public String getManName() {
        return "WCCOAjavadrv"+manNum;
    }    
    
    public void startup() {   
        if ( apiEnabled ) {
            new Thread(this).start();
        }
    }    

    @Override
    public void run() {
        apiStartup(args);
    }
    
//    @Override
//    public Transformation newTransformation(int type) {
//        return transformationFactory.newTransformation(type);
//    }    

    @Override
    public void answer4DpId(int index, Variable var) {
//        JDebug.out.log(Level.INFO, "answer4DpId {0}: {1}", new Object[]{index, var.formatValue()});
    }

    @Override
    public void hotLink2Internal(int index, Variable var) {
//        JDebug.out.log(Level.INFO, "hotLink2Internal {0}: {1}", new Object[]{index, var.formatValue()});
    }

    @Override
    public boolean initialize(String[] argv) {
        return true;
    }

    @Override
    public boolean start() {
        return true;
    }    
    
    @Override
    public void stop() {
    }    
//    
//    @Override
//    public HWObject workProc() {      
//        return null;
//    }            

// Communication Flow:
//    WCCOAjavadrv2:2016.09.09 15:06:57.160 com.etm.api.base.JTransformation.getVariableTypeAsInt        INFO   : getVariableTypeAsInt
//    WCCOAjavadrv2:2016.09.09 15:06:57.262 com.etm.api.base.JTransformation.toPeriph                    INFO   : toPeriph: dlen=4 var=100 subindex=0
//    WCCOAjavadrv2:2016.09.09 15:06:57.364 com.etm.api.base.JTransformation.toPeriph                    INFO   : toPeriph: done
//    WCCOAjavadrv2:2016.09.09 15:06:57.365 com.etm.api.base.JDriver.writeData                           INFO   : writeData: address=test1 subix=0 orgTime=Fri Sep 09 15:06:57 CEST 2016 data=[B@3c0a29df
//    WCCOAjavadrv2:2016.09.09 15:06:57.366 com.etm.api.base.JDriver.flushHW                             INFO   : flushHW

    @Override
    public void addDpPa(DpIdentifierVar dpid, String addr, byte direction) {
        addDpPa(dpid, addr, JDpAttrAddrDirection.values()[direction]);
    }
    @Override
    public void clrDpPa(DpIdentifierVar dpid, String addr, byte direction) {
        clrDpPa(dpid, addr, JDpAttrAddrDirection.values()[direction]);
    }
    
    protected abstract void addDpPa(DpIdentifierVar dpid, String addr, JDpAttrAddrDirection direction);
    protected abstract void clrDpPa(DpIdentifierVar dpid, String addr, JDpAttrAddrDirection direction);

    @Override
    public void notifyDisableCommands(boolean dc) {
        this.outputDisabled=dc;
    }

}
