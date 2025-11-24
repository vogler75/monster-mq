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
import at.rocworks.oa4j.var.TimeVar;

import java.util.List;
import at.rocworks.oa4j.var.Variable;
import java.util.ArrayList;
import java.util.Date;

/**
 *
 * @author vogler
 */
public class JDpSet extends JHotLinkWaitForAnswer {
    TimeVar time=null;
    List<JDpVCItem> vcs;
    boolean sent = false;

    public JDpSet async() {
        this.setAsync();
        return this;
    }      
    
    public JDpSet() {
        super();
        this.vcs = new ArrayList<>();
    }
    
    public JDpSet add(JDpVCItem vc) {
        this.vcs.add(vc);
        return this;
    }

    public JDpSet add(DpIdentifierVar dpid, Variable var) {
        this.vcs.add(new JDpVCItem(dpid, var));
        return this;
    }

    public JDpSet add(String dp, Variable var) {
        this.vcs.add(new JDpVCItem(new DpIdentifierVar(dp, "_original.._value"), var));
        return this;
    }

    public JDpSet add(String dp, Object var) {
        return add(dp, Variable.newVariable(var));
    }

    public JDpSet timed(TimeVar time) {
        this.time=time;
        return this;
    }
    
    public JDpSet timed(Date time) {
        this.time=new TimeVar(time);
        return this;
    }
    
    public JDpSet send() {
        this.sent=true;
        JManager.getInstance().enqueueHotlink(this); 
        return this;
    }        
    
    @Override
    public JDpSet action(IAnswer answer) {
        super.action(answer);
        return this;
    }

    public JDpSet answer(IAnswer answer) {
        return action(answer);
    }

    public JDpMsgAnswer await() {
        if ( !sent ) this.send();
        this.waitForAnswer();
        this.sent=false;
        return this.getAnswer();
    }

    @Override
    protected int execute() {
        int ret;
        JDpVCItem[] arr = new JDpVCItem[vcs.size()];
        for (int i=0; i<vcs.size(); i++) arr[i]=vcs.get(i);
        if (this.time==null) {
            if ((ret = JManager.getInstance().apiDpSet(this, arr)) == 0) 
                this.register();
        } else { // dpSetTimed
            if ((ret = JManager.getInstance().apiDpSetTimed(this, this.time, arr)) == 0) 
                this.register();            
        }
        return ret;
    }

    @Override
    protected void answer(JDpMsgAnswer answer) {
        this.deregister();
        super.answer(answer);
    }
}
