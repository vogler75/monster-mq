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

import java.util.ArrayList;

/**
 *
 * @author vogler
 */
public abstract class JDpQueryConnect extends JHotLinkWaitForAnswer {
    String query;
    boolean values=false; // wantsAnswer
    
    private class ConnectGroup {
        String query;
        long cptr;
        long cid;

        public ConnectGroup(String query, long cptr, long cid) {
            this.query=query;
            this.cptr=cptr;
            this.cid=cid;
        }
    }
    private final ArrayList<ConnectGroup> addList = new ArrayList<>();
    private final ArrayList<ConnectGroup> delList = new ArrayList<>();
    private final ArrayList<ConnectGroup> conList = new ArrayList<>();  

    public JDpQueryConnect(String query) {
        this.query=query;
    }        
    
    public JDpQueryConnect async(boolean yesno) {
        this.setAsync(yesno);
        return this;
    }         
    
    @Override
    public JDpQueryConnect action(IHotLink hotlink) {
        super.action(hotlink);
        return this;
    }
    
    @Override
    public JDpQueryConnect action(IAnswer answer) {
        super.action(answer);
        this.values=true;
        return this;
    }    
    
    public JDpQueryConnect connect() {
        if (query != null) {
            synchronized (conList) {
                addList.add(new ConnectGroup(query, 0, 0));
                if (addList.size() == 1) {
                    JManager.getInstance().enqueueHotlink(this);
                }
            }
        }
        return this;
    }    
    
    public void disconnect() {
        synchronized (conList) {
            boolean enqueue=(delList.isEmpty());
            delList.addAll(conList);
            if (enqueue) JManager.getInstance().enqueueHotlink(this);
        }
    }
    
    protected abstract int apiDpQueryConnect(JHotLinkWaitForAnswer hdl, Boolean values, String query);       
    
    @Override
    protected int execute() {
        int ret = 0;
        synchronized ( conList ) {
            for ( ConnectGroup c: addList ) {
                this.cptr=0;
                this.cid=0;
                if ( (ret = apiDpQueryConnect(this, this.values, c.query)) == 0 ) {
                    c.cptr=this.cptr;
                    c.cid=this.cid;
                    conList.add(c);                
                }
            }            
            addList.clear();
            for ( ConnectGroup c: delList ) {
                this.cptr=c.cptr;
                this.cid=c.cid;
                if ( (ret = JManager.getInstance().apiDpQueryDisconnect(this)) == 0 ) {
                    conList.remove(c);
                }
            }
            delList.clear();
            
            if ( conList.size() > 0 ) 
                this.register();
            else
                this.deregister();
        }
        return ret;
    }
}
