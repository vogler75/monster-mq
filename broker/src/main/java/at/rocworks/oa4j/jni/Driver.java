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
package at.rocworks.oa4j.jni;

import at.rocworks.oa4j.var.TimeVar;
import at.rocworks.oa4j.var.DpIdentifierVar;
import at.rocworks.oa4j.var.Variable;

/**
 *
 * @author vogler
 */
public abstract class Driver {    
    
    public native String apiGetLogPath();
    public native String apiGetDataPath();
    public native String apiGetConfigValue(String key);

    public native void apiStartup(String[] argv);
    
    public abstract void answer4DpId(int index, Variable var);
    public abstract void hotLink2Internal(int index, Variable var);
    public abstract boolean initialize(String[] argv);
    public abstract boolean start();
    public abstract void stop();
    public abstract HWObject readData();
    public abstract boolean writeData(String address, int trans, byte[] data, int subix, TimeVar time);
    public abstract void flushHW();
    public abstract void notifyDisableCommands(boolean dc);
    
    public abstract Transformation newTransformation(String name, int type);
    
    // direction:
    // 1.. output unsolicted
    // 2.. input unsolicited
    // 3.. input single query
    // 4...input polling
    // 6...inout unsolicited
    // 7...inout polling
    // 8...inout single query
    public abstract void addDpPa(DpIdentifierVar dpid, String addr, byte direction);
    public abstract void clrDpPa(DpIdentifierVar dpid, String addr, byte direction);        
}
