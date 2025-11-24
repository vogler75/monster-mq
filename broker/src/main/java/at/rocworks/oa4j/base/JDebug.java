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

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.*;

/**
 *
 * @author vogler
 */
public class JDebug {
    
    public static final java.util.logging.Logger out = java.util.logging.Logger.getLogger(JDebug.class.getName());
    public static final String fmtDate = "yyyy.MM.dd HH:mm:ss.SSS";

    private static class LogFormatter extends Formatter {
        @Override
        public String format(LogRecord record) {
            SimpleDateFormat fmt = new SimpleDateFormat(JDebug.fmtDate);
            String formattedMessage = formatMessage(record);
            return fmt.format(new Date()) + " " + String.format("%-60s %-7s", record.getSourceClassName() + "." + record.getSourceMethodName(), record.getLevel()) + ": " + formattedMessage + "\n";
        }
    }

    public static boolean setConsole() {
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new LogFormatter());
        out.addHandler(handler);
        out.setUseParentHandlers(false);
        return true;
    }

    public static boolean setOutput(String fqn)  {
        try {
            FileHandler fh = new FileHandler(fqn + ".%g.log", 5242880, 5, true);
            fh.setFormatter(new LogFormatter());
            out.addHandler(fh);
            out.setUseParentHandlers(false);
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    public static void setOutput(String path, String filename)  {
        if (!setOutput(path+filename))
            setOutput(filename);
    }
    
    public static void setLevel(Level level) {
        JDebug.out.setLevel(level);
    }    
    
    public static void StackTrace(Level level, Exception ex) {
        String trace = ex.toString()+":\n";
        for (StackTraceElement ste : ex.getStackTrace()) {
                trace += "  " + ste.getClassName() + "." + ste.getMethodName() + ":" + ste.getLineNumber()+"\n";
        }
        JDebug.out.log(level, trace);
    }    
    
    public static void StackTrace(Level level, String msg) {
        String trace = msg+":\n";
        for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
                trace += "  " + ste.getClassName() + "." + ste.getMethodName() + ":" + ste.getLineNumber()+"\n";
        }
        JDebug.out.log(level, trace);
    }        
    
    public static void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ex) {
            JDebug.StackTrace(Level.SEVERE, ex);
        }
    }    
}

//-----------------------------------------------------------------------------------------
