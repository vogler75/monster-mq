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
import java.util.Arrays;

/**
 *
 * @author vogler
 */
public abstract class JHotLinkWaitForAnswerGroup extends JHotLinkWaitForAnswer {

    protected class ConnectGroup {
        ArrayList<String> dps;
        long cptr;

        public ConnectGroup() {
            this.dps = new ArrayList<>();
            this.cptr = 0;
        }
        public ConnectGroup(ArrayList<String> dps) {
            this.dps = dps;
            this.cptr = 0;
        }        
        public ConnectGroup(ArrayList<String> dps, long cptr) {
            this.dps = dps;
            this.cptr = cptr;
        }
    }

    protected final ArrayList<ConnectGroup> addList = new ArrayList<>();
    protected final ArrayList<ConnectGroup> delList = new ArrayList<>();
    protected final ArrayList<ConnectGroup> conList = new ArrayList<>();
    
    protected synchronized JHotLinkWaitForAnswerGroup addGroup() {
        addList.add(new ConnectGroup());
        return this;
    }    
          
    protected synchronized JHotLinkWaitForAnswerGroup addGroup(String[] dps) {
        addList.add(new ConnectGroup(new ArrayList<>(Arrays.asList(dps))));
        return this;
    }
    
    protected synchronized JHotLinkWaitForAnswerGroup addGroup(ArrayList<String> dps) {
        addList.add(new ConnectGroup(dps));
        return this;
    }
    
    protected synchronized JHotLinkWaitForAnswerGroup add(String dp) {
        if (addList.isEmpty()) addGroup();
        addList.get(addList.size()-1).dps.add(dp);
        return this;
    }
    
    protected synchronized JHotLinkWaitForAnswerGroup connect() {
        if (!addList.isEmpty()) {
            JManager.getInstance().enqueueHotlink(this);
        }        
        return this;
    }

    public synchronized void disconnect() {
        if (!conList.isEmpty()) {
            delList.addAll(conList);
            JManager.getInstance().enqueueHotlink(this);
        }
    }    
}
