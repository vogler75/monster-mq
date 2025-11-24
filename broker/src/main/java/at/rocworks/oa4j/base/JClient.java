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
package at.rocworks.oa4j.base;

import at.rocworks.oa4j.var.*;
//import javafx.util.Pair;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author vogler
 */
public class JClient {
    
    public static boolean isConnected() {
        return JManager.getInstance()!=null && JManager.getInstance().isConnected();
    }

    // dpGet
    
    public static JDpGet dpGet() {
        return (new JDpGet());
    }

    public static Variable dpGet(String dp) {
        JDpMsgAnswer answer = (new JDpGet()).add(dp).await();
        return ( answer.size()>0 ) ? answer.getItem(0).getVariable() : null;
    }

    public static int dpGet(String dp, VariablePtr var) {
        JDpMsgAnswer answer = (new JDpGet()).add(dp).await();
        if ( answer.size()>0 ) var.set(answer.getItem(0).getVariable());
        return answer.getRetCode();
    }

    public static List<Variable> dpGet(List<String> dps) {
        JDpGet dpGet = new JDpGet();
        dps.forEach((dp)->dpGet.add(dp));
        JDpMsgAnswer answer = dpGet.await();
        List<Variable> ret = new ArrayList<Variable>(answer.size());
        answer.forEach((item)->ret.add(item.getVariable()));
        return ret;
    }

    // dpGetPeriod
    public static JDpGetPeriod dpGetPeriod(TimeVar start, TimeVar stop, int num) {
        return (new JDpGetPeriod(start, stop, num));
    }

    public static JDpGetPeriod dpGetPeriod(Date start, Date stop, int num) {
        return dpGetPeriod(new TimeVar(start), new TimeVar(stop), num);
    }

    public static JDpGetPeriod dpGetPeriod(Long start, long stop, int num) {
        return dpGetPeriod(new TimeVar(start), new TimeVar(stop), num);
    }

    // dpSet
    public static JDpSet dpSet() {
        return (new JDpSet());
    }

    public static JDpSet dpSet(String dp, Object var) {
        return (new JDpSet()).add(dp, Variable.newVariable(var)).send();
    }

    public static int dpSetWait(String dp, Object var) {
        return dpSet(dp, var).await().getRetCode();
    }

    // dpConnect
    public static JDpConnect dpConnect() {
        return (new JDpConnect());
    }
    
    // alertConnect
    public static JAlertConnect alertConnect() {
        return (new JAlertConnect());
    }    
    
    // dpQuery
    public static JDpQuery dpQuery(String query) {
        return (new JDpQuery(query).send());
    }

    // dpQueryConnect
    public static JDpQueryConnect dpQueryConnectSingle(String query) {
        return (new JDpQueryConnectSingle(query));
    }
    
    public static JDpQueryConnect dpQueryConnectAll(String query) {
        return (new JDpQueryConnectAll(query));
    }

    // dpNames
    public static String[] dpNames(String pattern) {
        return (String[])JManager.getInstance().executeTask(()->{
            return JManager.getInstance().apiGetIdSet(pattern);
        });
    }
    
    public static String[] dpNames(String pattern, String type) {
        return (String[])JManager.getInstance().executeTask(()->{
            return JManager.getInstance().apiGetIdSetOfType(pattern, type);
        });
    }    
    
    // dpComment
    public static LangTextVar dpGetComment(DpIdentifierVar dpid) {
        return (LangTextVar)JManager.getInstance().executeTask(()->{
            return JManager.getInstance().apiDpGetComment(dpid);
        });
    }

    /**
     * Verfiy password. Check if the given passwd is valid for the requested user id
     * @param username
     * @param password
     * @return 0...Ok, -1...invalid user, -2...wrong password
     */
    public static int checkPassword(String username, String password) {
        return (int)JManager.getInstance().executeTask(()->{
            return JManager.getInstance().checkPassword(username, password);
        });
    }

    /**
     * A new user id is set when (id matches passwd) or
     * (currentId is ROOT_USER and newUserId exists) or
     * (newUserId is DEFAULT_USER).
     * @param username
     * @param password
     * @return true if user has been set
     */
    public static boolean setUserId(String username, String password) {
        return (boolean)JManager.getInstance().executeTask(()->{
            return JManager.getInstance().setUserId(username, password);
        });
    }
}
