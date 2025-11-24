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

import java.util.Date;

import at.rocworks.oa4j.var.BitVar;
import at.rocworks.oa4j.var.DpIdentifierVar;
import at.rocworks.oa4j.var.IntegerVar;
import at.rocworks.oa4j.var.TimeVar;
import at.rocworks.oa4j.var.FloatVar;
import at.rocworks.oa4j.var.TextVar;
import at.rocworks.oa4j.var.Variable;
import at.rocworks.oa4j.var.VariableType;

/**
 *
 * @author vogler
 */
public class JDpVCItem {
    private final DpIdentifierVar dpid;
    private final Variable var;
    private final long time;

    /**
     * AnswerItem Constructor
     *
     * @param dpid
     * @param var
     * @param time
     */
    public JDpVCItem(DpIdentifierVar dpid, Variable var, long time) {
        this.dpid = dpid;
        this.var = var;
        this.time = time;
    }

    /**
     * DpVCItem Constructor
     *
     * @param dpid
     * @param var
     */
    public JDpVCItem(DpIdentifierVar dpid, Variable var) {
        this(dpid, var, 0);
    }

    public DpIdentifierVar getDpIdentifier() {
        return dpid;
    }

    public String getDpName() {
        return dpid.getName();
    }

    public Variable getVariable() {
        return var;
    }

    public Object getValueObject() {
        return var.getValueObject();
    }

    /**
     * get timestamp as ms since epoch
     *
     * @return timestamp as ms since epoch
     */
    public long getTime() {
        return time;
    }

    /**
     * get timestamp as date
     *
     * @return timestamp as date
     */
    public Date getDate() {
        return new Date(time);
    }

    public String getValueClassName() {
        return var.getValueClassName();
    }

    public VariableType isA() {
        return var.isA();
    }

    public int getVariableTypeAsNr() {
        return var.getVariableTypeAsNr();
    }

    @Override
    public String toString() {
        return "["+this.dpid + "," + this.var + "," + this.var.isA() + ","+this.time+"]";
    }
}
