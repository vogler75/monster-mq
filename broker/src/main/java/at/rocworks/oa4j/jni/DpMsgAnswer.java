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

/**
 *
 * @author vogler
 */
public class DpMsgAnswer extends Msg {
    public DpMsgAnswer() {
        super();
    }
    
    public DpMsgAnswer(long cptr) {
        super.setPointer(cptr);
    }       
    
    public DpMsgAnswer(Msg message) {
        this.cptr=newFromMsg(message.getPointer());        
    }
    
    public DpMsgAnswer(DpMsgAnswer answer) {
        this.cptr=newFromMsgAnswer(answer.getPointer());        
    }    
    
    public native boolean insertGroup(AnswerGroup group);
    //public native void setOutstandingProgress(int percents); // TODO: not available in IOWA
    
    protected native long newFromMsg(long dpMsgPtr);        
    protected native long newFromMsgAnswer(long dpMsgPtr);                
}
