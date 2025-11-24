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

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author vogler
 */
public class JDpGetPeriod extends JHotLinkWaitForAnswer {
    private TimeVar tStart, tStop;
    private int num;
    private List<DpIdentifierVar> dps;
    private boolean sent = false;

    public JDpGetPeriod(TimeVar tStart, TimeVar tStop, int num) {
        super();
        this.tStart=tStart;
        this.tStop=tStop;
        this.num=num;
        this.dps=new ArrayList<>();
    }

    public JDpGetPeriod add(String dp) {
        dps.add(new DpIdentifierVar(dp, "_offline.._value"));
        return this;
    }

    public JDpGetPeriod add(DpIdentifierVar dpid) {
        dps.add(dpid);
        return this;
    }

    public JDpGetPeriod async() {
        this.setAsync();
        return this;
    }

    public JDpGetPeriod send() {
        this.sent = true;
        JManager.getInstance().enqueueHotlink(this);
        return this;
    }

    public JDpMsgAnswer await() {
        if (!sent) this.send();
        this.waitForAnswer();
        this.sent = false;
        return this.getAnswer();
    }

    @Override
    public JDpGetPeriod action(IAnswer answer) {
        super.action(answer);
        return this;
    }

    @Override
    protected int execute() {
        int ret;
        DpIdentifierVar[] arr = new DpIdentifierVar[dps.size()];
        for (int i=0; i<dps.size(); i++) arr[i]=dps.get(i);
        if ((ret = JManager.getInstance().apiDpGetPeriod(this, tStart, tStop, num, arr)) == 0)
            this.register();
        return ret;
    }

    @Override
    protected void answer(JDpMsgAnswer answer) {
        this.deregister();
        super.answer(answer);
    }
}