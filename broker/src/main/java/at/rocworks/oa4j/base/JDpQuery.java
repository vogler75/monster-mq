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

/**
 *
 * @author vogler
 */
public class JDpQuery extends JHotLinkWaitForAnswer {
    String query;
    boolean sent = false;    
         
    public JDpQuery(String query) {
        super();
        this.query = query;
    }
    
    public JDpQuery async() {
        this.setAsync();
        return this;
    }          
    
    public JDpQuery send() {
        this.sent=true;
        JManager.getInstance().enqueueHotlink(this);        
        return this;
    }
        
    public JDpMsgAnswer await() {
        if ( !sent ) this.send();
        this.waitForAnswer();
        this.sent=false;
        return this.getAnswer();
    }   
    
    @Override
    public JDpQuery action(IAnswer answer) {
        super.action(answer);
        return this;
    }

    @Override
    protected int execute() {
        int ret;
        if ( (ret=JManager.getInstance().apiDpQuery(this, query)) == 0 ) 
            this.register();
        return ret;
    }

    @Override
    protected void answer(JDpMsgAnswer answer) {
        this.deregister();
        super.answer(answer);
    }
}
