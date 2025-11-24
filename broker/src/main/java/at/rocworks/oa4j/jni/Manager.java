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

import at.rocworks.oa4j.base.JDpVCItem;
import at.rocworks.oa4j.base.JHotLinkWaitForAnswer;
import at.rocworks.oa4j.var.DpIdentifierVar;
import at.rocworks.oa4j.var.DynVar;
import at.rocworks.oa4j.var.LangTextVar;
import at.rocworks.oa4j.var.TimeVar;
import at.rocworks.oa4j.var.Variable;

/**
 *
 * @author vogler
 */
public abstract class Manager {
    public static native String apiGetVersion(); // Runtime Version (PVSS_VERSION)

    private static Boolean _isV4 = null;
    private static Boolean _isV3 = null;

    public static boolean isV3() {
        return (_isV3==null ? _isV3=(apiGetVersion().startsWith("3.")) : _isV3);
    }

    public static boolean isV4() {
        return (_isV4==null ? _isV4=(apiGetVersion().startsWith("4.")) : _isV4);
    }

    public native String apiGetLogPath();
    public native String apiGetDataPath();
    public native String apiGetConfigValue(String key);

    public int apiStartup(int manType, String[] argv) {
        return apiStartup(manType, argv, true, true, true, false);
    }
    public native int apiStartup(int manType, String[] argv,
                                 boolean connectToData, boolean connectToEvent,
                                 boolean initResources, boolean debugFlag);
    public native int apiShutdown();
    
    public native void apiDispatch(int sec, int usec);    
    
    public native int apiDpGet(JHotLinkWaitForAnswer hdl, DpIdentifierVar[] dps);
    public native int apiDpSet(JHotLinkWaitForAnswer hdl, JDpVCItem[] dps);
    public native int apiDpSetTimed(JHotLinkWaitForAnswer hdl, TimeVar originTime, JDpVCItem[] dps);
    public native int apiDpQuery(JHotLinkWaitForAnswer hdl, String query);
    public native int apiDpGetPeriod(JHotLinkWaitForAnswer hdl, TimeVar start, TimeVar stop, int num, DpIdentifierVar[] dps);

    public native int apiDpConnect(JHotLinkWaitForAnswer hdl, String dp);
    public native int apiDpDisconnect(JHotLinkWaitForAnswer hdl, String dp);
    
    public native int apiDpConnectArray(JHotLinkWaitForAnswer hdl, String[] dps);    
    public native int apiDpDisconnectArray(JHotLinkWaitForAnswer hdl, String[] dps);    
    
    public native int apiDpQueryConnectSingle(JHotLinkWaitForAnswer hdl, boolean values, String query);
    public native int apiDpQueryConnectAll(JHotLinkWaitForAnswer hdl, boolean values, String query);
    public native int apiDpQueryDisconnect(JHotLinkWaitForAnswer hdl);
    
    public native int apiAlertConnect(JHotLinkWaitForAnswer hdl, String[] dps);
    public native int apiAlertDisconnect(JHotLinkWaitForAnswer hdl, String[] dps);
    
    public native String[] apiGetIdSet(String pattern);    
    public native String[] apiGetIdSetOfType(String pattern, String type);    
    
    public native LangTextVar apiDpGetComment(DpIdentifierVar dp);

    /**
     * Verfiy password. Check if the given passwd is valid for the requested user id
     * @param username
     * @param password
     * @return 0...Ok, -1...invalid user, -2...wrong password
     */
    public native int checkPassword(String username, String password);

    /**
     * A new user id is set when (id matches passwd) or
     * (currentId is ROOT_USER and newUserId exists) or
     * (newUserId is DEFAULT_USER).
     * @param username
     * @param password
     * @return true if user has been set
     */
    public native boolean setUserId(String username, String password);
    
    public native void apiDoReceiveSysMsg(long cPtrSysMsg);
    public native void apiDoReceiveDpMsg(long cPtrDpMsg);
    
    public native int apiSendArchivedDPs(DynVar elements, boolean isAlert);
    
    private native void apiSetManagerState(int state);
    public void apiSetManagerState(ManagerState state) {
        apiSetManagerState(state.value);
    }
       
    // callbacks from API   
    public abstract boolean doReceiveSysMsg(long cPtrSysMsg);    
    public abstract boolean doReceiveDpMsg(long cPtrDpMsg);

    public int callbackAnswer(int id, int idx) {    
        return callbackAnswer(id, idx, null, null, 0);
    }
    abstract public int callbackAnswer(int id, int idx, DpIdentifierVar dpid, Variable var, long time);
    abstract public int callbackAnswerError(int id, int code, String text);

    public int callbackHotlink(int id, int idx) {
        return callbackHotlink(id, idx, null, null);
    }
    abstract public int callbackHotlink(int id, int idx, DpIdentifierVar dpid, Variable var);  
    
    public native int apiProcessHotlinkGroup(int id, long ptrDpHlGroup);
    public abstract int callbackHotlinkGroup(int id, long ptrDpHlGroup);

    public native int apiGetConnectionState();
    public native int apiIsActiveConnection();

    public native void apiLog(int prio, long state, String text);
}
