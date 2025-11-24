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

import at.rocworks.oa4j.var.TimeVar;

/**
 *
 * @author vogler
 */
public class HWObject {
    public String address;    
    public byte[] data;       
    public TimeVar orgTime;
    public boolean timeOfPeriphFlag;  
    
    public HWObject() {
        this.address=null;
        this.data=null;        
        this.orgTime=null;
        this.timeOfPeriphFlag=false;
    }
    
    public HWObject(String address, byte[] data, TimeVar orgTime, boolean timeOfPeriphFlag) {
        this.address=address;
        this.data=data;        
        this.orgTime=orgTime;     
        this.timeOfPeriphFlag=timeOfPeriphFlag;
    }    
}
