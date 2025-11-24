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

import at.rocworks.oa4j.base.JDebug;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Date;
import java.util.logging.Level;

/**
 *
 * @author vogler
 */
public class JDriverItem implements Serializable {

    protected String name; // Address
    protected int ttnr; // TransformationTypeNr
    protected Date time; // Original Time
    protected byte[] data;    
    
    public JDriverItem() {
        name = null;
        ttnr = 0; // undefined
        data = null;
        time = null;
    }

    // Periph --> Driver --> WinCC OA
    public JDriverItem(String name, byte[] data) {
        this.name = name;
        this.ttnr = 0; // undefined
        this.data = data;
        this.time = null;
    }
    
    public JDriverItem(String name, byte[] data, Date time) {
        this.name = name;
        this.ttnr = 0; // undefined
        this.data = data;
        this.time = time;
    }    

    // WinCC OA --> Driver --> Periph
    public JDriverItem(String name, int ttnr, byte[] data, Date time) {
        this.name = name;
        this.ttnr = ttnr;
        this.data = data;
        this.time = time;
    }

    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name=name;
    }    
    
    public int getTransTypeNr() {
        return ttnr;
    }
    
    public byte[] getData() {
        return data;
    }

    public Date getTime() {
        return time;
    }    

    @Override
    public String toString() {
        return "name=" + (name == null ? "null" : name)
            + " time=" + (time == null ? "null" : time)
            + " data=" + (data == null ? "null" : data);
    }

//    public byte[] toBytesDataOnly() {
//        return data;
//    }

//    public static JDriverItem fromBytesDataOnly(String name, byte[] bytes) {
//        return fromBytesDataOnly(name, bytes, null);
//    }
//    
//    public static JDriverItem fromBytesDataOnly(String name, byte[] bytes, Date time) {
//        return new JDriverItem(name, bytes, time);
//    }    

    public byte[] toBytes() {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(this);
            return bos.toByteArray();
        } catch (IOException ex) {
            JDebug.StackTrace(Level.SEVERE, ex);
            return null;
        }
    }

    public static JDriverItem fromBytes(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInput in = new ObjectInputStream(bis)) {
            Object obj = in.readObject();
            if (obj instanceof JDriverItem) {
                return (JDriverItem) obj;
            } else {
                return null;
            }
        } catch (IOException | ClassNotFoundException ex) {
            JDebug.StackTrace(Level.SEVERE, ex);
            return null;
        }
    }        
}
