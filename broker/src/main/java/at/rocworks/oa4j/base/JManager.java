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

import at.rocworks.oa4j.jni.*;
import at.rocworks.oa4j.var.DpIdentifierVar;
import at.rocworks.oa4j.var.Variable;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * @author vogler
 */
public class JManager extends Manager implements Runnable {
    public static final int DB_MAN = 3;        
    public static final int API_MAN = 7;

    public static int MAX_ENQUEUE_SIZE_HIGH = 100000;
    public static int MAX_ENQUEUE_SIZE_LOW = 50000;
    public static int MAX_DEQUEUE_SIZE_HIGH = 10000;  // Message queue high threshold (full capacity)
    public static int MAX_DEQUEUE_SIZE_LOW = 50000;    // Message queue low threshold (recovery point)

    private boolean connectToData = true;
    private boolean connectToEvent = true;

    private volatile int maxEnqueueSizeReached = 0;
    private volatile long lastEnqueueFullLogTime = 0;
    private static final long ENQUEUE_LOG_INTERVAL_MS = 1000;

    private static JManager instance = null; // Singleton
    
    protected JSemaphore loopPaused = new JSemaphore(true);
    protected volatile boolean apiEnabled = false;
    protected volatile boolean apiConnected = false;    
    protected volatile boolean loopBreak = false;

    private final ConcurrentHashMap<Integer, JHotLinkWaitForAnswer> hotlinkList = new ConcurrentHashMap<>();    
    //private final ConcurrentLinkedQueue<Callable> hotlinkQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Callable> taskQueue = new ConcurrentLinkedQueue<>();
    
    private int loopWaitUSec=10000;
    
    private String projName="<unknown>";
    private String projDir=".";        
    private String confDir="."; 
    
    private int manType=API_MAN;
    private int manNum=1;

    private boolean initResources=true;
    private boolean debugFlag=false;

    private Map<String, String> initSysMsgData;

    /**
     * Sets the maximum enqueue size thresholds for the task queue.
     * When the queue reaches the high threshold, new hotlinks are discarded.
     * When the queue drops below the low threshold, normal processing resumes.
     *
     * @param high Upper threshold at which hotlinks start being discarded
     * @param low  Lower threshold at which normal processing resumes
     * @return This JManager instance for method chaining
     */
    public JManager setMaxEnqueueSize(int high, int low) {
        MAX_ENQUEUE_SIZE_HIGH = high;
        MAX_ENQUEUE_SIZE_LOW = low;
        return this;
    }

    /**
     * Sets the maximum dequeue size thresholds for message queue processing.
     * Controls the flow of message processing from WinCC OA.
     *
     * @param high Upper threshold for message queue (full capacity)
     * @param low  Lower threshold for message queue (recovery point)
     * @return This JManager instance for method chaining
     */
    public JManager setMaxDequeueSize(int high, int low) {
        MAX_DEQUEUE_SIZE_HIGH = high;
        MAX_DEQUEUE_SIZE_LOW = low;
        return this;
    }

    /**
     * Returns the current size of the task queue.
     * Useful for monitoring queue load and detecting potential overload situations.
     *
     * @return The number of tasks currently in the queue
     */
    public int getEnqueueSize() {
        return taskQueue.size();
    }

    /**
     * Returns the singleton instance of the JManager.
     * Only one JManager instance can exist per JVM.
     *
     * @return The singleton JManager instance, or null if not yet initialized
     */
    public static JManager getInstance() {
        return JManager.instance;
    }
    
    /**
     * Returns the project directory path.
     *
     * @return The absolute path to the WinCC OA project directory
     */
    public String getProjPath() { return projDir; }
    private JManager setProjPath(String projDir) { 
        this.projDir=projDir; 
        this.confDir=this.projDir+"/config";                 
        return this; 
    }        
    
    /**
     * Sets the WinCC OA project name.
     *
     * @param projName The name of the WinCC OA project to connect to
     * @return This JManager instance for method chaining
     */
    public JManager setProjName(String projName) {
        this.projName=projName;
        return this;
    }
    
    /**
     * Returns the configuration directory path.
     *
     * @return The absolute path to the project's config directory
     */
    public String getConfigDir() { return confDir; }

    /**
     * Returns the log directory path.
     *
     * @return The absolute path to the project's log directory
     */
    public String getLogDir() { return apiGetLogPath(); }

    /**
     * Returns the full path to this manager's log file.
     *
     * @return The absolute path to the manager's log file (without extension)
     */
    public String getLogFile() { return getLogDir()+getManName(); }

    /**
     * Retrieves a configuration value from the WinCC OA config file.
     *
     * @param key The configuration key to look up (e.g., "java:classPath")
     * @return The configuration value, or null if not found
     */
    public String getConfigValue(String key) { return apiGetConfigValue(key); }

    /**
     * Retrieves a configuration value with a default fallback.
     *
     * @param key The configuration key to look up
     * @param def The default value to return if the key is not found or empty
     * @return The configuration value, or the default if not found or empty
     */
    public String getConfigValueOrDefault(String key, String def) {
        String val = apiGetConfigValue(key);
        return (val==null || val.isEmpty()) ? def : val;
    }
    
    /**
     * Checks if the WinCC OA native API is enabled.
     * The API is enabled after the native library has been successfully loaded.
     *
     * @return true if the native API library is loaded, false otherwise
     */
    public boolean isEnabled() { return apiEnabled; }

    /**
     * Checks if the manager is connected to WinCC OA.
     * The manager is connected after start() completes and until stop() is called.
     *
     * @return true if the manager is currently connected to WinCC OA
     */
    public boolean isConnected() { return apiConnected; }
    
    /**
     * Returns the manager type.
     *
     * @return The manager type constant (API_MAN or DB_MAN)
     */
    public int getManType() { return manType; }
    private JManager setManType(int manType) { this.manType=manType; return this; }

    /**
     * Returns the manager number.
     * Multiple managers of the same type can run with different numbers.
     *
     * @return The manager number (default is 1)
     */
    public int getManNum() { return manNum; }

    /**
     * Sets the manager number.
     * Used to distinguish multiple instances of the same manager type.
     *
     * @param manNum The manager number to set
     * @return This JManager instance for method chaining
     */
    public JManager setManNum(int manNum) { this.manNum=manNum; return this; }
    
    /**
     * Sets the wait time for the main dispatch loop in microseconds.
     * Controls how long the manager waits during each dispatch cycle.
     * Lower values increase responsiveness but use more CPU.
     *
     * @param usec Wait time in microseconds (default is 10000)
     * @return This JManager instance for method chaining
     */
    public JManager setLoopWaitUSec(int usec) {
        this.loopWaitUSec=usec;
        return this;
    }

    /**
     * Returns the current wait time for the main dispatch loop.
     *
     * @return The wait time in microseconds
     */
    public int getLoopWaitUSec() {
        return this.loopWaitUSec;
    }

    /**
     * Initializes the manager by parsing command-line arguments.
     * This is the primary initialization method for managers started from the command line.
     * <p>
     * Supported arguments:
     * <ul>
     *   <li>{@code -path <dir>} - Set the project directory path</li>
     *   <li>{@code -proj <name>} - Set the project name (required)</li>
     *   <li>{@code -num <n>} - Set the manager number</li>
     *   <li>{@code -db} - Use DB_MAN manager type instead of API_MAN</li>
     *   <li>{@code -noinit} - Skip resource initialization</li>
     *   <li>{@code -debug} - Enable debug output</li>
     * </ul>
     *
     * @param args Command-line arguments array
     * @return This JManager instance for method chaining
     * @throws Exception If the native library cannot be loaded or version is incompatible
     */
    public JManager init(String args[]) throws Exception {
        for ( int i=0; i<args.length; i++ ) {
            // projDir & configDir
            if ( args[i].equals("-path") && args.length>i+1 ) {
                setProjPath(args[i+1]);        
            }
            
            if ( args[i].equals("-proj") && args.length>i+1 ) {
                setProjName(args[i+1]);
            }            
                        
            // managerNum
            if ( args[i].equals("-num") && args.length>i+1 ) {
                setManNum(Integer.parseInt(args[i+1]));
            }
            
            // managerType
            if ( args[i].equals("-db") ) {
                setManType(DB_MAN);
            }

            // initResources
            if ( args[i].equals("-noinit") ) {
                initResources=false;
            }

            // debug
            if ( args[i].equals("-debug")) {
                debugFlag=true;
            }
        }        
        return init();
    }

    /**
     * Initializes the manager with explicit configuration parameters.
     * Use this method for programmatic initialization without command-line arguments.
     *
     * @param projName The WinCC OA project name
     * @param manType  The manager type (API_MAN or DB_MAN)
     * @param manNum   The manager number
     * @return This JManager instance for method chaining
     * @throws Exception If the native library cannot be loaded or version is incompatible
     */
    public JManager init(String projName, int manType, int manNum) throws Exception {
        setProjName(projName);        
        setManType(manType);
        setManNum(manNum);    
        return init();
    }    

    private JManager init() throws Exception {
        if (JManager.instance == null) {
            JManager.instance = this;
        } else {
            throw new IllegalStateException("There can only be one manager!");
        }

        apiEnabled=false;
        String errmsg1="";
        String errmsg2="";
        try {
            System.loadLibrary("WCCOAjava");
            apiEnabled=true;
        } catch ( java.lang.UnsatisfiedLinkError ex1 ) {
            errmsg1=ex1.getMessage();
            try {
                System.loadLibrary("WCCILjava"); // this was for ManagerV4 (IOWA)
                apiEnabled=true;
            } catch ( java.lang.UnsatisfiedLinkError ex2 ) {
                errmsg2=ex2.getMessage();
            }            
        }

        if ( apiEnabled ) {
            // Set log file settings
            if (!isV3() && !isV4()) {
                throw new Exception("Manager Version "+apiGetVersion()+" is not V3 and not V4!");
            }
        } else {
            if (!errmsg1.isEmpty())
                throw new Exception(errmsg1);
            if (!errmsg2.isEmpty())
                throw new Exception(errmsg2);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                JManager.log(ErrPrio.PRIO_INFO, ErrCode.NOERR, "Shutdown in one second...");
                Thread.sleep(1000);
                stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                stackTrace(ErrPrio.PRIO_WARNING, ErrCode.UNEXPECTEDSTATE, e);
            }
        }));

        return this;
    }

    /**
     * Returns the manager name used for identification and logging.
     *
     * @return The manager name in format "WCCOAjava{manNum}"
     */
    public String getManName() {
        return "WCCOAjava"+manNum;
    }

    /**
     * Starts the manager with default connection settings.
     * Connects to both the data manager and event manager.
     * This method blocks until the manager is fully started and connected.
     */
    public void start() {
        start(true, true);
    }

    /**
     * Starts the manager with specified connection options.
     * This method blocks until the manager is fully started and connected.
     *
     * @param connectToData  If true, connect to the data manager for datapoint operations
     * @param connectToEvent If true, connect to the event manager for alerts and events
     */
    public void start(boolean connectToData, boolean connectToEvent) {
        if ( apiEnabled ) {
            log(ErrPrio.PRIO_INFO, ErrCode.MANAGER_START, "Manager start...");
            this.connectToData=connectToData;
            this.connectToEvent=connectToEvent;
            new Thread(this).start();
            loopPaused.awaitFalse();
            log(ErrPrio.PRIO_INFO, ErrCode.MANAGER_INITIALIZED, "Manager started.");
        }
    }

    /**
     * Stops the manager and disconnects from WinCC OA.
     * This method should be called before application exit to ensure clean shutdown.
     * Pauses the dispatch loop and shuts down the native API connection.
     */
    public void stop() {
        if ( apiEnabled ) {
            log(ErrPrio.PRIO_INFO, ErrCode.MANAGER_STOP, "Manager stop.");
            apiConnected = false; // stop run loop
            pause();
            apiShutdown();
        }
    }        

    @Override
    public void run() {
        apiStartup(manType,
                new String[]{"WCCILjava", "-proj", projName, "-num", Integer.toString(manNum)},
                connectToData, connectToEvent,
                initResources, debugFlag);
        loopPaused.sendFalse();
        apiConnected=true;
        while (apiConnected) {
            log(ErrPrio.PRIO_INFO, ErrCode.NOERR, "Manager loop waiting.");
            loopPaused.awaitFalse();
            log(ErrPrio.PRIO_INFO, ErrCode.NOERR, "Manager loop started.");
            while ( !loopBreak ) {
                synchronized ( this ) {
                    apiDispatch(0, loopWaitUSec);
                    queueWorker();
                }
            }
            loopPaused.sendTrue();
            log(ErrPrio.PRIO_INFO, ErrCode.NOERR, "Manager loop stopped.");
        }
    }

    /**
     * Pauses the manager's dispatch loop.
     * While paused, no new messages are processed from WinCC OA.
     * Use resume() to continue processing.
     * This method blocks until the loop is fully paused.
     */
    public void pause() {
        loopBreak=true;
        loopPaused.awaitTrue();
    }

    /**
     * Resumes the manager's dispatch loop after a pause.
     * Message processing continues from where it was paused.
     */
    public void resume() {
        loopBreak=false;
        loopPaused.sendFalse();
    }    
    
    protected void enqueueHotlink(JHotLinkWaitForAnswer hl) {
        if (taskQueue.size() >= MAX_ENQUEUE_SIZE_HIGH) {
            // OVERLOAD: Discard hotlink, log once per second
            maxEnqueueSizeReached++;
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastEnqueueFullLogTime >= ENQUEUE_LOG_INTERVAL_MS) {
                log(ErrPrio.PRIO_WARNING, ErrCode.NOERR,
                    "Max enqueue size reached " + taskQueue.size() + " discarding hotlink...");
                lastEnqueueFullLogTime = currentTime;
            }
        } else if (maxEnqueueSizeReached > 0 && taskQueue.size() <= MAX_ENQUEUE_SIZE_LOW) {
            // RECOVERY: Below threshold, add hotlink and report recovery
            taskQueue.add(hl);
            maxEnqueueSizeReached = 0;
            lastEnqueueFullLogTime = 0;
            log(ErrPrio.PRIO_WARNING, ErrCode.NOERR,
                "Enqueue below threshold size " + taskQueue.size() + " processing hotlink...");
        } else if (maxEnqueueSizeReached == 0) {
            // NORMAL: Not in overload, add hotlink
            taskQueue.add(hl);
        }
        // IMPLICIT: else {} when overload && size > LOW - hotlink silently discarded during recovery phase
    }   
    
    protected void register(JHotLinkWaitForAnswer hl) {
        hotlinkList.put(hl.getHdlId(), hl);
    }
    
    protected void deregister(JHotLinkWaitForAnswer hl) {
        hotlinkList.remove(hl.getHdlId());
    }
    
    private void queueWorker() {        
        int k;
        try {
            // Maximum of loop, otherwise it could happen that we are in this
            // loop forever, if concurrently hotlink-requests are added...
            Callable task;
            for ( k=0; k<=100 && (task=taskQueue.poll()) != null; k++ ) {
                task.call();
            }
        } catch (Exception ex) {
            stackTrace(ErrPrio.PRIO_SEVERE, ErrCode.UNEXPECTEDSTATE, ex);
        }                        
    }
    
    /**
     * Adds a task to the manager's task queue for asynchronous execution.
     * Tasks are executed in the main dispatch loop thread.
     * This method returns immediately; use executeTask() for synchronous execution.
     *
     * @param task The Callable to execute
     * @return true if the task was successfully added to the queue
     */
    public boolean enqueueTask(Callable task) {
        return taskQueue.add(task);
    }

    /**
     * Executes a task synchronously in the manager's dispatch loop thread.
     * This method blocks until the task completes and returns its result.
     * Use this when you need to execute code in the WinCC OA context and wait for the result.
     *
     * @param task The Callable to execute
     * @return The result of the task execution, or null if the task returned no result
     */
    public Object executeTask(Callable task) {
        ArrayList<Object> res = new ArrayList<>();
        JSemaphore sem = new JSemaphore(false);
        enqueueTask(()->{
            res.add(task.call());
            sem.sendTrue();
            return true;
        });        
        sem.awaitTrue();
        return ( res.size() > 0 ? res.get(0) : null);        
    }
    
    @Override
    public int callbackAnswer(int id, int idx, DpIdentifierVar dpid, Variable var, long time) {
        JHotLinkWaitForAnswer hdl;
        hdl = hotlinkList.get(id);
        if (hdl != null) {                      
            switch (idx) {
                case -1:
                    hdl.answerInit();
                    break;
                case -2:
                    hdl.callbackDone();
                    break;
                default:
                    hdl.callbackItem(new JDpVCItem(dpid, var, time));
                    break;
            }
            return 0;
        } else 
            return -1;
    }

    @Override
    public int callbackAnswerError(int id, int code, String text) {
        JHotLinkWaitForAnswer hdl;
        hdl = hotlinkList.get(id);
        if (hdl != null) {
            hdl.answerError(code, text);
        }
        return 0;
    }

    @Override
    public int callbackHotlink(int id, int idx, DpIdentifierVar dpid, Variable var) {
        JHotLinkWaitForAnswer hdl;
        hdl = hotlinkList.get(id);
        if (hdl != null) {
            switch (idx) {
                case -1:
                    hdl.hotlinkInit();
                    break;
                case -2:
                    hdl.callbackDone();
                    break;
                default:
                    hdl.callbackItem(new JDpVCItem(dpid, var, 0));
                    break;
            }
            return 0;
        } else {
            return -1;
        }
    }           
           
    @Override
    public boolean doReceiveSysMsg(long cPtrSysMsg) {
        SysMsg msg = new SysMsg(cPtrSysMsg);
//        JDebug.out.info("------------SYSMSG DEBUG BEGIN-------------------------");
//        JDebug.out.log(Level.INFO, "isA => {0}", msg.isA());
//        JDebug.out.info(msg.toDebug(99));
//        JDebug.out.info("------------SYSMSG DEBUG END  -------------------------");
        if (msg.getSysMsgType() == msg.getSysMsgTypes().INIT_SYS_MSG()) {
            initSysMsgData = msg.getInitSysMsgData();
        } else if (msg.getSysMsgType() == msg.getSysMsgTypes().REDUNDANCY_SYS_MSG()) {
            if (msg.getSourceManType()== ManagerType.EVENT_MAN) { // msg comes twice, from data and event manager
                switch (msg.getSysMsgRedundancySubType()) {
                    case REDUNDANCY_ACTIVE: becameActive(); break;
                    case REDUNDANCY_PASSIVE: becamePassive(); break;
                }
            }
        }
        return false;
    }

    private int isActive=-1; // -1...unknown, 0...no, 1...true

    /**
     * Checks if the manager is connected to the active host in a redundant system.
     * In non-redundant systems, this always returns true.
     * The state is updated automatically via redundancy system messages.
     *
     * @return true if connected to the active host, false if connected to passive host
     */
    public Boolean isActive()
    {
        if (isActive==-1) // if it is unknown take the state from the InitSysMsg
            return (getInitSysMsgData().getOrDefault("general:active", "1").equals("1"));
        else
            return isActive==1;
    }

    /**
     * executed when manager becomes actigve
     */
    protected void becameActive() {
        isActive=1; // active
    }

    /**
     * executed when manager becomes passive
     */
    protected void becamePassive() {
        isActive=0; // passive
    }

    /**
     * Returns the initialization system message data.
     * Contains information received during manager initialization, including
     * system state, redundancy information, and configuration details.
     *
     * @return A map of key-value pairs from the InitSysMsg, or an empty map if not yet received
     */
    public Map<String, String> getInitSysMsgData()
    {
        return initSysMsgData==null ? new HashMap<String, String>() : initSysMsgData;
    }


    @Override
    public boolean doReceiveDpMsg(long cPtrDpMsg) {
//        DpMsg msg = new DpMsg(cPtrDpMsg);
//        JDebug.out.info("------------DPMSG DEBUG BEGIN-------------------------");
//        JDebug.out.log(Level.INFO, "isA => {0}", msg.isA());
//        JDebug.out.info(msg.toDebug(99));
//        JDebug.out.info("------------DPMSG DEBUG END  -------------------------");
        return false;
    }
    
    // NOT USED
    
    @Override
    public int callbackHotlinkGroup(int id, long ptrDpHlGroup) {
        apiProcessHotlinkGroup(id, ptrDpHlGroup);
        return 0;
    }

    /**
     * Logs a message to the WinCC OA log system.
     * Messages are written to the manager's log file and may appear in the WinCC OA console
     * depending on the priority level.
     *
     * @param prio Priority level (e.g., PRIO_INFO, PRIO_WARNING, PRIO_SEVERE)
     * @param code Error code describing the state or error type
     * @param text The message text to log
     */
    public static void log(ErrPrio prio, ErrCode code, String text) {
        JManager instance = getInstance();
        if (instance != null) {
            instance.apiLog(prio.getValue(), code.getValue(), text);
        }
    }

    /**
     * Logs an exception's stack trace to the WinCC OA log system.
     * Formats the exception with class name, message, and full stack trace.
     *
     * @param prio      Priority level for the log message
     * @param code      Error code describing the error state
     * @param exception The exception to log (if null, method returns without logging)
     */
    public static void stackTrace(ErrPrio prio, ErrCode code, Throwable exception) {
        if (exception == null) {
            return;
        }

        // Get the stack trace as a string
        StringBuilder sb = new StringBuilder();
        sb.append(exception.getClass().getName()).append(": ").append(exception.getMessage()).append("\n");

        for (StackTraceElement element : exception.getStackTrace()) {
            sb.append("  at ").append(element).append("\n");
        }

        // Remove trailing newline
        if (sb.length() > 0 && sb.charAt(sb.length() - 1) == '\n') {
            sb.setLength(sb.length() - 1);
        }

        log(prio, code, sb.toString());
    }

    /**
     * Logs an exception's stack trace with SEVERE priority and UNEXPECTEDSTATE error code.
     * Convenience method for logging unexpected exceptions during execution.
     *
     * @param exception The exception to log (if null, method returns without logging)
     */
    public static void stackTrace(Throwable exception) {
        stackTrace(ErrPrio.PRIO_SEVERE, ErrCode.UNEXPECTEDSTATE, exception);
    }
}
