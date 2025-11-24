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

import at.rocworks.oa4j.jni.ErrCode;
import at.rocworks.oa4j.jni.ErrPrio;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author vogler
 */
public class JSemaphore implements Serializable {

    private int value;

    public JSemaphore() { value = 0; }
    public JSemaphore(boolean state) {
        value = state ? 1 : 0;
    }

    // Boolean Semaphore

    /**
     * Set Digital Semaphore to TRUE
     */
    public synchronized void dispatch() {
        value=1;
        this.notify();
    }

    /**
     * Wait for Digital Semapohre becomes TURE and set it afterwards to FALSE
     */
    public synchronized void request() {
        while (value==0) {
            try {
                this.wait();
            } catch (InterruptedException ex) {
                JManager.stackTrace(ex);
            }
        }
        value=0;
    }

    /**
     * Wait for Digital Semapohre becomes TRUE and set it afterwards to FALSE with a timeout
     * @param timeout .. time to wait in ms
     * @return true .. timedout
     */
    public synchronized boolean request(int timeout) { // Digital.request(int timeout)
        if (timeout <= 0) {
            this.request();
            return false; // no timeout
        } else {
            boolean timedout = false;
            while (value==0 && !timedout) {
                try {
                    this.wait(timeout);
                } catch (InterruptedException ex) {
                    JManager.stackTrace(ex);
                }
                timedout = (value==0);
            }
            value = 0;
            return timedout;
        }
    }

    /**
     * Get current state of boolean semaphore
     * @return state of semaphore
     */
    public synchronized boolean getState() { return this.value>1; }

    /**
     * Set current state of boolean semaphore to FALSE
     */

    public synchronized void sendFalse() {
        value = 0;
        this.notifyAll();
    }

    /**
     * Set current state of boolean semaphore to TRUE
     */
    public synchronized void sendTrue() {
        value = 1;
        this.notifyAll();
    }

    /**
     * Wait until boolean semaphore becomes FALSE
     */
    public synchronized void awaitFalse() {
        while (value!=0) {
            try {
                this.wait();
            } catch (InterruptedException ex) {
                JManager.stackTrace(ex);
            }
        }
    }

    /**
     * Wait until boolean semaphore becomes TRUE
     */
    public synchronized void awaitTrue() {
        while (value==0) {
            try {
                this.wait();
            } catch (InterruptedException ex) {
                JManager.stackTrace(ex);
            }
        }
    }

    // Analog Semaphore

    /**
     * Wait until analog semaphore becomes greater than given value
     * @param value ... greater than this value
     */
    public synchronized void awaitGT(int value) {
        while (value>this.value) {
            try {
                this.wait();
            } catch (InterruptedException ex) {
                JManager.stackTrace(ex);
            }
        }
    }

    /**
     * Wait until analog semaphore becomes less than or equal than given value
     * @param value
     */
    public synchronized void awaitLTE(int value) {
        while (value<=this.value) {
            try {
                this.wait();
            } catch (InterruptedException ex) {
                JManager.stackTrace(ex);
            }
        }
    }

    /**
     * Get current value of analog semaphore
     * @return value of  semaphore
     */
    public synchronized int getValue() {
        return this.value;
    }

    /**
     * Set current value of analog semaphore
     * @param value of semaphore
     * @return value of semaphore
     */
    public synchronized int setValue(int value) {
        this.value = value;
        this.notify();
        return this.value;
    }

    /**
     * Adds one to the analog semaphore
     * @return new value of semaphore
     */
    public synchronized int  addOne() {
        this.value++;
        this.notify();
        return this.value;
    }

    /**
     * Waits for at least one semaphore and removes one
     * @return new value of semaphore
     */

    public synchronized int getOne() {
        while (this.value==0) {
            try {
                this.wait();
            } catch (InterruptedException ex) {
                JManager.stackTrace(ex);
            }
        }
        this.value--;
        this.notify();
        return this.value;
    }
}