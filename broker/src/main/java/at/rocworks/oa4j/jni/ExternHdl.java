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

import java.io.IOException;
import at.rocworks.oa4j.base.JDebug;
import at.rocworks.oa4j.var.Variable;

/**
 *
 * @author vogler
 */
public class ExternHdl {
      
    public static native String apiGetLogDir();
    public static native int apiGetManType();        
    public static native int apiGetManNum();
    public static native int apiAddResult(long cptr, Variable result);

    /**
     * Start Control Function
     * @param cptr WaitCondPtr
     * @param name Functionname
     * @param args Arguments
     * @return EXEC_OK=0, EXEC_ERROR=1, EXEC_DONE=2
     */
    public static native int apiStartFunc(long cptr, String name, Variable args);

    public static void init() throws IOException {
        String name;
        int type = apiGetManType();
        switch (type) {
            case 4: name="ui"; break;
            case 5: name="ctrl"; break;
            case 7: name="api"; break;
            default: name="java";
        }
        JDebug.setOutput(apiGetLogDir(), "WCCOA"+name+apiGetManNum()+"java");
        //JDebug.out.info("java ExternHdl init done. ");
    }            
}