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
package at.rocworks.oa4j.driver;

import at.rocworks.oa4j.jni.HWObject;
import at.rocworks.oa4j.var.TimeVar;
import at.rocworks.oa4j.base.JDpAttrAddrDirection;
import at.rocworks.oa4j.jni.Transformation;
import at.rocworks.oa4j.var.DpIdentifierVar;
import at.rocworks.oa4j.base.JDebug;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.logging.Level;

/**
 *
 * @author vogler
 */
public abstract class JDriverSimple extends JDriver {                     
    
    // Addresses
    private final HashMap<String, Integer> addrInUse = new HashMap<>(); // Addr, Count
    
    // Inputs
    private final HashMap<String, Integer> addrInPending  = new HashMap<>(); // Addr, Count
    private final HashMap<String, Integer> addrInAttached = new HashMap<>(); // Addr, Count 
    
    // Outputs
    private final HashMap<String, Integer> addrOutPending  = new HashMap<>(); // Addr, Count
    private final HashMap<String, Integer> addrOutAttached = new HashMap<>(); // Addr, Count
        
    protected final LinkedBlockingQueue<JDriverItemList> inputQueue;
    protected final LinkedBlockingQueue<JDriverItemList> outputQueue;

    protected JDriverItemList currentInput=null;
    protected JDriverItemList currentOutput=new JDriverItemList();        

    private final HWObject hw = new HWObject();

    protected volatile int inputCounter=0;                
    protected volatile int outputCounter=0;
    
    protected volatile int droppedInputItems=0;
    
    protected volatile Date inputLastWorkTime = new Date(0);
    protected volatile Date outputLastWorkTime = new Date(0);
    
    protected int inputPerformance=0;
    protected int outputPerformance=0;

    protected int statsUpdateSec = 1;
    private Thread statisticsThread;
    private Thread outputQueueWorkerThread;
    
    public JDriverSimple(String[] args) throws Exception {
        this(args, 10);
    }
    
    public JDriverSimple(String[] args, int statsUpdateSeconds) throws Exception {
        this(args, statsUpdateSeconds, 256, 256);
    }

    public JDriverSimple(String[] args, int statsUpdateSeconds, int maxInputQueueSize, int maxOutputQueueSize) throws Exception {
        super(args);
        this.statsUpdateSec=statsUpdateSeconds;
        inputQueue = new LinkedBlockingQueue<>(maxInputQueueSize);
        outputQueue = new LinkedBlockingQueue<>(maxOutputQueueSize);
    }

    @Override
    public boolean start() {
        (statisticsThread=new Thread(()->statisticsThread())).start();
        (outputQueueWorkerThread=new Thread(()->outputQueueWorker())).start();
        attachAddresses();
        return super.start();
    }     

    @Override
    public void stop() {
        statisticsThread.interrupt();
        outputQueueWorkerThread.interrupt();
        super.stop(); 
        
    }        

        
    protected void statisticsThread() {
        try {
            Date t1 = new Date();
            SimpleDateFormat fmt = new SimpleDateFormat(TimeVar.FMT_DATE_JS_MS);
            while ( statsUpdateSec > 0 ) {
                Thread.sleep(statsUpdateSec*1000);
                Date t2 = new Date();
                long d = t2.getTime()-t1.getTime();
                inputPerformance=(int)Math.ceil(1000.0*inputCounter/d);
                outputPerformance=(int)Math.ceil(1000.0*outputCounter/d);
                int inputPerformanceDropped=(int)Math.ceil(1000.0*droppedInputItems/d);
                inputCounter=0;
                outputCounter=0;
                droppedInputItems=0;
                t1=t2;
                JDebug.out.log(Level.INFO, "v/s in: {0} ({1}) out: {2} | queue in: {3} ({4}) out: {5} | time in: {6} out: {7}",
                        new Object[]{inputPerformance, inputPerformanceDropped, outputPerformance, 
                            inputQueue.size(), droppedInputBlocks, outputQueue.size(),
                            fmt.format(inputLastWorkTime), fmt.format(outputLastWorkTime),
                            });
                droppedInputBlocks=0;
            }
        } catch (InterruptedException ex) {
        }            
    }
    
    @Override
    public Transformation newTransformation(String name, int type) {
        switch ( type ) {
            case 1000: return new JTransTextVar(name, type); 
            case 1001: return new JTransIntegerVar(name, type); 
            case 1002: return new JTransFloatVar(name, type); 
            default: 
                JDebug.out.log(Level.WARNING, "unhandled transformation type {0} for {1}", new Object[]{type, name});
                return null;
        }        
    }
    
    protected JDriverItem newOutputItem(String address, int trans, byte[] data, Date time) {
        return new JDriverItem(address, trans, data, time);
    }        

    @Override
    public HWObject readData() {                 
        if (currentInput==null) {               
            if ( (currentInput=inputQueue.poll())==null ) 
                return null;
        }

        // driver framework sends the data from bottom to top (of list)! (don't know why)
        // so, we push the data from bottom to top to have the correct order 
        JDriverItem item = currentInput.pollLast(); 
        if (item==null) {
            currentInput=null;
            return null;
        }
        
        inputCounter++;
        inputLastWorkTime=new Date();        

        hw.address=item.getName();
        hw.data=item.getData();
        if ( item.getTime() != null ) {
            hw.timeOfPeriphFlag=true;                
            hw.orgTime=new TimeVar(item.getTime());
        } else {
            hw.timeOfPeriphFlag=false;
            hw.orgTime=null;
        }            

        return hw;
    }

    @Override
    public boolean writeData(String address, int trans, byte[] data, int subix, TimeVar time) {
        //JDebug.out.log(Level.INFO, "writeData: address={0} subix={1} orgTime={2} data={3} dlen={4}", new Object[]{address, subix, time.getTime(), data, data.length});
        outputCounter++;
        outputLastWorkTime=new Date();
        currentOutput.addItem(newOutputItem(address, trans, data, time.getValue()));
        return true;
    }
        
    @Override
    public void flushHW() {
        try {
            //JDebug.out.log(Level.INFO, "flushHW "+currentOutput.getSize());
            outputQueue.add(currentOutput);
        } catch (IllegalStateException ex) { // Queue Full
            JDebug.StackTrace(Level.SEVERE, ex);
        }
        currentOutput=new JDriverItemList();
    }
    
    protected void outputQueueWorker() {
        JDriverItemList data;
        try {        
            while (true) {
                if ( (data=takeOutputBlock())!=null ) {
                    sendOutputBlock(data);
                }
            }
        } catch (InterruptedException ex) {
            
        }            
    }
    
    private volatile int droppedInputBlocks=0;
    protected boolean sendInputBlock(JDriverItemList block) {
        try {
            return inputQueue.add(block);            
        } catch (IllegalStateException ex) { // Queue Full
            droppedInputBlocks++;
            droppedInputItems+=block.getSize();
            return false;
        }
    }

    public JDriverItemList peekOutputBlock() throws InterruptedException {
        return outputQueue.peek();
    }

    public JDriverItemList takeOutputBlock() throws InterruptedException {
        return outputQueue.take();
    }    

    public boolean isAddrInUse(String addr) {
        return this.addrInUse.containsKey(addr);
    }     

    protected boolean attachAddress(String addr) {return true;}
    protected boolean detachAddress(String addr) {return true;}
    
    protected abstract boolean attachInput(String addr);
    protected abstract boolean detachInput(String addr);    
    
    protected abstract boolean attachOutput(String addr);
    protected abstract boolean detachOutput(String addr);  
    
    public abstract void sendOutputBlock(JDriverItemList data);

    public void attachAddresses() {
        JDebug.out.info("attachAddresses");
        addrInPending.forEach((addr, count)->{
            if ( count > 0 ) {
                handleAttachAddr(addrInAttached, addr, this::attachInput, count);
            } else if ( count < 0 ) {
                handleDetachAddr(addrInAttached, addr, this::attachInput, count);
            }
        });
        addrOutPending.forEach((addr, count)->{
            if ( count > 0 ) {
                handleAttachAddr(addrOutAttached, addr, this::attachOutput, count);
            } else if ( count < 0 ) {
                handleDetachAddr(addrOutAttached, addr, this::attachOutput, count);
            }
        });        
    }
        
    public void lostAllAddresses() {
        addrInPending.clear();
        addrInAttached.forEach((addr, count)->addrInPending.put(addr, count));
        addrInAttached.clear();
        
        addrOutPending.clear();
        addrOutAttached.forEach((addr, count)->addrOutPending.put(addr, count));
        addrOutAttached.clear();        
    }
        
    @Override
    public void addDpPa(DpIdentifierVar dpid, String addr, JDpAttrAddrDirection direction) {
        try {
            //JDebug.out.log(Level.INFO, "addDpPa dpid={0} addr={1} direction={2}", new Object[]{dpid, addr, direction});
            switch (direction) {
            case INPUT_SPONT:
                if (!handleAttachAddr(addrInAttached, addr, this::attachInput, 1))
                    addrInPending.put(addr, addrInPending.getOrDefault(addr, 0)+1);
                break;
            case OUTPUT:
                if (!handleAttachAddr(addrOutAttached, addr, this::attachOutput, 1))
                    addrOutPending.put(addr, addrOutPending.getOrDefault(addr, 0)+1);                
                break;
            }
        } catch (Exception ex) {
            JDebug.StackTrace(Level.SEVERE, ex);
        }                        
    }

    @Override
    public void clrDpPa(DpIdentifierVar dpid, String addr, JDpAttrAddrDirection direction) {
        try {
            //JDebug.out.log(Level.INFO, "clrDpPa dpid={0} addr={1} direction={2}", new Object[]{dpid, addr, direction});
            switch (direction) {
            case INPUT_SPONT:
                if (!handleDetachAddr(addrInAttached, addr, this::detachInput, 1)) 
                    addrInPending.put(addr, addrInPending.getOrDefault(addr, 0)-1);                    
                break;
            case OUTPUT:
                if (!handleDetachAddr(addrOutAttached, addr, this::detachOutput, 1))
                    addrOutPending.put(addr, addrOutPending.getOrDefault(addr, 0)-1);                
                break;              
            }
            //JDebug.out.log(Level.INFO, "clrDpPa {0} {1} ... done", new Object[]{dpid, addr});
        } catch (Exception ex) {
            JDebug.StackTrace(Level.SEVERE, ex);
        }              
    }    

    private boolean updateAddressUsage(String addr, int change) {
        boolean done=true;
        
        int countOld = addrInUse.getOrDefault(addr, 0);
        int countNew = countOld+change;
        
        if (countOld<=0 && countNew>0) 
            done=attachAddress(addr);
        else if (countOld>0 && countNew<=0)
            done=detachAddress(addr);
        
        if (done) 
            addrInUse.put(addr, countNew);        
        return done;
    }
    
    private boolean handleAttachAddr(HashMap<String, Integer> pool, String addr, Function<String, Boolean> attach, int count) {
        boolean done=false;
        synchronized (pool) {
            Integer c=pool.getOrDefault(addr, 0);
            if (c==0) {
                try {
                    if ( updateAddressUsage(addr, count) && attach.apply(addr) ) {
                        pool.put(addr, count);
                        //JDebug.out.log(Level.INFO, "handleAttachAddr addr={0} ... connected", new Object[]{addr});
                        done=true;
                    } 
                } catch (Exception ex) {
                    JDebug.StackTrace(Level.SEVERE, ex);
                }
            } else {
                pool.put(addr, c+count);
                //JDebug.out.log(Level.INFO, "handleAttachAddr addr={0} ... connect count={1}", new Object[]{addr, c+count});
                done=true;
            }
            return done;
        }
    }    
    
    private boolean handleDetachAddr(HashMap<String, Integer> pool, String addr, Function<String, Boolean> detach, int count) {
        boolean done=false;
        synchronized (pool) {
            Integer c=pool.get(addr);
            if (c==null) {
                // does not exist, nothing to do
                done=true;
            } else if ( c==0 ) {
                pool.remove(addr);
                //JDebug.out.log(Level.INFO, "clrDpPa addr={0} ... removed", new Object[]{addr});
                done=true;
            } else if ( c-count<=0 ) {
                try {
                    if ( detach.apply(addr) ) {
                        pool.remove(addr);
                        //JDebug.out.log(Level.INFO, "clrDpPa addr={0} ... disconnected", new Object[]{addr});
                        done=true;
                    } 
                } catch (Exception ex) {
                    JDebug.StackTrace(Level.SEVERE, ex);
                }
            }
            else { // c>0
                pool.put(addr, -count);
                //JDebug.out.log(Level.INFO, "clrDpPa addr={0} ... connect count={1}", new Object[]{addr, c});
                done=true;
            }
            if (done) {
                updateAddressUsage(addr, -count);
            }            
            return done;
        }
    }
}
