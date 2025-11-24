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

import at.rocworks.oa4j.var.DpIdentifierVar;

import java.util.ArrayList;

/**
 *
 * @author vogler
 */
public class JDpConnect extends JHotLinkWaitForAnswerGroup {        
    
    public JDpConnect async() {
        this.setAsync();
        return this;
    }

    public JDpConnect answer(IAnswer answer) { return action(answer); }

    public JDpConnect hotlink(IHotLink hotlink) { return action(hotlink); }
    
    @Override
    public JDpConnect action(IHotLink hotlink) {
        return (JDpConnect)super.action(hotlink);
    }
    
    @Override
    public JDpConnect action(IAnswer answer) {
        return (JDpConnect)super.action(answer);
    }

    @Override
    public JDpConnect addGroup() {
        return (JDpConnect)super.addGroup();
    }    
          
    @Override
    public JDpConnect addGroup(String[] dps) {
        return (JDpConnect)super.addGroup(dps);
    }
    
    @Override
    public JDpConnect addGroup(ArrayList<String> dps) {
        return (JDpConnect)super.addGroup(dps);
    }
    
    @Override
    public JDpConnect add(String dp) {
        return (JDpConnect)super.add(DpIdentifierVar.addConfigIfNotExists(dp, "_online.._value"));
    }
    
    @Override
    public JDpConnect connect() {
        return (JDpConnect)super.connect();
    }     
    
    @Override
    protected int execute() {
        int ret = 0;
        for (ConnectGroup c : addList) {
            this.cptr = 0;
            if ((ret = JManager.getInstance().apiDpConnectArray(this, c.dps.toArray(new String[c.dps.size()]))) == 0) {
                c.cptr = this.cptr;
                conList.add(c);
            }
        }
        addList.clear();
        for (ConnectGroup c : delList) {
            this.cptr = c.cptr;
            if ((ret = JManager.getInstance().apiDpDisconnectArray(this, c.dps.toArray(new String[c.dps.size()]))) == 0) {
                conList.remove(c);
            }
        }
        delList.clear();
        if (conList.size() > 0)
            this.register();
        else
            this.deregister();
        
        return ret;
    }        
}
