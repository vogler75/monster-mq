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

import java.util.ArrayList;

/**
 *
 * @author vogler
 */
public class JAlertConnect extends JHotLinkWaitForAnswerGroup {    

    @Override
    public JAlertConnect action(IHotLink hotlink) {
        return (JAlertConnect)super.action(hotlink);
    }
    
    @Override
    public JAlertConnect action(IAnswer answer) {
        return (JAlertConnect)super.action(answer);
    }       
    
    @Override
    public JAlertConnect addGroup() {
        return (JAlertConnect)super.addGroup();
    }    
          
    @Override
    public JAlertConnect addGroup(String[] dps) {
        return (JAlertConnect)super.addGroup(dps);
    }
    
    @Override
    public JAlertConnect addGroup(ArrayList<String> dps) {
        return (JAlertConnect)super.addGroup(dps);
    }
    
    @Override
    public JAlertConnect add(String dp) {
        return (JAlertConnect)super.add(dp);
    }
    
    @Override
    public JAlertConnect connect() {
        return (JAlertConnect)super.connect();
    }         
    
    @Override
    public int execute() {
        int ret = 0;
        
        for (ConnectGroup c : addList) {
            this.cptr = 0;
            if ((ret = JManager.getInstance().apiAlertConnect(this, c.dps.toArray(new String[c.dps.size()]))) == 0) {
                c.cptr = this.cptr;
                conList.add(c);
            }
        }
        addList.clear();
        for (ConnectGroup c : delList) {
            this.cptr = c.cptr;
            if ((ret = JManager.getInstance().apiAlertDisconnect(this, c.dps.toArray(new String[c.dps.size()]))) == 0) {
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

    @Override
    public void hotlink(JDpHLGroup group) {
        if ( this.cbHotlink != null) 
            this.cbHotlink.hotlink(group);
    }

    @Override
    public void answer(JDpMsgAnswer answer) {
        if (this.cbAnswer != null)
            this.cbAnswer.answer(answer);
    }
}