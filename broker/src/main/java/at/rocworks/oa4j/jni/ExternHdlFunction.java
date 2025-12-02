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

import at.rocworks.oa4j.base.JManager;
import at.rocworks.oa4j.var.DynVar;
import at.rocworks.oa4j.var.TextVar;
import at.rocworks.oa4j.var.Variable;

import java.util.logging.Level;

/**
 *
 * @author vogler
 */
abstract public class ExternHdlFunction implements Runnable {
    private final long waitCondPtr; // c++ pointer to waitCond object

    private boolean isAsync = false;
    private volatile boolean done = false;

    private TextVar function;
    private DynVar parameter;

    public ExternHdlFunction(long waitCondPtr) {
        this.waitCondPtr = waitCondPtr;
    }

    /**
     * Internal, is called from the JNI in the case of an async function
     * @param function Function name which will be passed to execute function
     * @param parameter Parameters which will be passed to execute function
     */
    public void start(TextVar function, DynVar parameter) {
        //JDebug.out.log(Level.INFO, "start function={0} parameter={1}", new Object[] { function, parameter.formatValue() });
        this.isAsync=true;
        this.function=function;
        this.parameter=parameter;
        new Thread(this).start();
    }

    /**
     * Function which will be executed from WinCC OA Control script
     * @param function Name of a (sub)program unit, must be handled by yourself (e.g. case statement)
     * @param parameter List of input parameter values
     * @return finished or not (if not finished the done flag may set set by a thread, only used for async calls)
     */
    abstract public DynVar execute(TextVar function, DynVar parameter);

    /**
     * Internal, called from JNI in the case of threaded function
     * @return true when thread is finished
     */
    public boolean checkDone() {
        return done;
    }

    public boolean isAsync() { return isAsync; }
    protected void setDone() { done=true; }

    /**
     * Add a result values in the case when the function is called async
     * @param var Variable which should be passed out
     */
    public void addResult(Variable var) throws IllegalStateException {
        if ( waitCondPtr > 0 )
            ExternHdl.apiAddResult(waitCondPtr, var);
        else
            throw new IllegalStateException("no waitCondPtr available!");
    }

    /**
     * Start Control Function / Callback to Control
     * @param name Functionname
     * @param args Arguments
     * @return EXEC_OK=0, EXEC_ERROR=1, EXEC_DONE=2, -99..not an async call
     */
    public int startFunc(String name, Variable args) throws IllegalStateException {
        if ( waitCondPtr > 0 )
            return ExternHdl.apiStartFunc(waitCondPtr, name, args);
        else
            throw new IllegalStateException("no waitCondPtr available!");
    }

    /**
     * Internal, called when function is called async
     */
    @Override
    public void run() {
        try {
            addResult(execute(function, parameter));
        } catch (Exception ex) {
            JManager.stackTrace(ex);
        }
    }
}
