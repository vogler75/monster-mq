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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author vogler
 */
public class SysMsg extends Msg {
    public SysMsg() {
        super();
    }
    
    public SysMsg(long cptr) {
        super(cptr);
    }   
    
    private native int getSysMsgType(long cptr);
    private native int getSysMsgSubType(long cptr);

    /**
     * get the SysMsgType
     * @return type nr of message
     */
    public int getSysMsgType() {
        return getSysMsgType(cptr);
    }

    /**
     * get the SysMsgRedundancySubType if the message is of type REDUNDANCY_SYS_MSG
     * @return SysMsgRedundancySubType of message or null if it is not of type REDUNDANCY_SYS_MSG
     */
    public SysMsgRedundancySubType getSysMsgRedundancySubType() {
        if (getSysMsgType() == getSysMsgTypes().REDUNDANCY_SYS_MSG())
            return SysMsgRedundancySubType.values()[getSysMsgSubType(cptr)];
        else
            return SysMsgRedundancySubType.NONE;
    }

    /**
     * get the data of the InitSysMsg if the message is a InitSysMsg
     * @return InitSysMsg data (key, value)
     */
    public native Map<String, String> getInitSysMsgData();

    protected static SysMsgType _systypes = null;

    public SysMsgType getSysMsgTypes() {
        if (_systypes==null) {
            if (Manager.isV3())
                _systypes=new SysMsgTypeV3();
            else if (Manager.isV4())
                _systypes=new SysMsgTypeV4();
        }
        return _systypes;
    }

    public interface SysMsgType {
        int NO_SYS_MSG();
        int KEEP_ALIVE();
        int INIT_SYS_MSG();
        int START_DP_INIT();
        int END_DP_INIT();
        int START_MANAGER();
        int SHUT_DOWN_MANAGER();
        int NAMESERVER_SYS_MSG();
        int OPEN_SERVER();
        int CLOSE_SERVER();

        int RECOVERY_SYS_MSG();
        int REDUNDANCY_SYS_MSG();
        int DIST_SYS_MSG();
        int FILETRANSFER_SYS_MSG();
    }

    public static class SysMsgTypeV3 implements SysMsgType {
        public int NO_SYS_MSG() { return 1; }
        public int KEEP_ALIVE() { return 2; }
        public int INIT_SYS_MSG() { return 3; }
        public int START_DP_INIT() { return 4; }
        public int END_DP_INIT() { return 5; }
        public int START_MANAGER() { return 6; }
        public int SHUT_DOWN_MANAGER() { return 7; }
        public int NAMESERVER_SYS_MSG() { return 8; }
        public int OPEN_SERVER() { return 9; }
        public int CLOSE_SERVER() { return 10; }
        public int LICENSE_SYS_MSG() { return 11; }
        public int RECOVERY_SYS_MSG() { return 12; }
        public int REDUNDANCY_SYS_MSG() { return 13; }
        public int DIST_SYS_MSG() { return 14; }
        public int FILETRANSFER_SYS_MSG() { return 15; }
    }

    public static class SysMsgTypeV4 implements SysMsgType {
        public int NO_SYS_MSG() { return 1; }
        public int KEEP_ALIVE() { return 2; }
        public int INIT_SYS_MSG() { return 3; }
        public int START_DP_INIT() { return 4; }
        public int END_DP_INIT() { return 5; }
        public int START_MANAGER() { return 6; }
        public int SHUT_DOWN_MANAGER() { return 7; }
        public int NAMESERVER_SYS_MSG() { return 8; }
        public int OPEN_SERVER() { return 9; }
        public int CLOSE_SERVER() { return 10; }
        public int RECOVERY_SYS_MSG() { return  11; }
        public int REDUNDANCY_SYS_MSG() { return  12; }
        public int DIST_SYS_MSG() { return  13; }
        public int FILETRANSFER_SYS_MSG() { return  14; }
        public int SEND_BUFFER() { return  15; }
        public int SERVICEROUTER_SYS_MSG() { return  16; }
        public int DELTA_SYS_MSG() { return  17; }
        public int CAL_SYS_MSG() { return  18; }
        public int PREPARE_RESTART_MSG() { return  19; }
        public int MANAGER_DISPATCH_SYS_MSG() { return  20; }
    }
}
