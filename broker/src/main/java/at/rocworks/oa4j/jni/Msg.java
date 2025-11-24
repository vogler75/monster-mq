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
public class Msg extends Malloc {

    public int getType() { return isA(this.cptr); }

    public Msg() {
        super();
    }
    
    public Msg(long cptr) {
        super.setPointer(cptr);
    }        

    protected native int isA(long cptr);

    public native long getMsgId();

    public void forwardMsg(ManagerType manType, int manNum) {
        forwardMsg(manType.value, manNum);
    }

    public native void forwardMsg(int manType, int manNum);

    public native void forwardMsgToData();

    public native int getSourceManTypeNr();

    public ManagerType getSourceManType() {
        return ManagerType.values()[getSourceManTypeNr()];
    }

    public native int getSourceManNum();
    
    @Override
    public native String toString();
    
    public native String toDebug(int level);

    @Override
    protected long malloc() {
        throw new UnsupportedOperationException("cannot instantiate abstract class"); 
    }

    @Override
    protected native void free(long cptr);

    protected static MsgType _types = null;
    public MsgType getMsgTypes() {
        if (_types==null) {
            if (Manager.isV3())
                _types=new MsgTypeV3();
            else if (Manager.isV4())
                _types=new MsgTypeV4();
        }
        return _types;
    }

    public interface MsgType {
         int NO_MSG ();
         int SYS_MSG ();
         int SYS_MSG_START_DPINIT ();
         int SYS_MSG_NAMESERVER ();
         int SYS_MSG_INIT ();
         int SYS_MSG_FILE_TRANSFER ();
         int DP_MSG ();
         int DP_MSG_INITCONFIG ();
         int DP_MSG_IDENTIFICATION ();
         int DP_MSG_TYPECONTAINER ();
         int DP_MSG_DPTYPE_REQ ();
         int DP_MSG_MANIP_DPTYPE ();
         int DP_MSG_DP_REQ ();
         int DP_MSG_MANIP_DP ();
         int DP_MSG_DPALIAS_REQ ();
         int DP_MSG_MANIP_DPALIAS ();
         int DP_MSG_CONNECTION ();
         int DP_MSG_CONNECT ();
         int DP_MSG_CONNECT_RET ();
         int DP_MSG_CONNECT_NOSOURCE ();
         int DP_MSG_DISCONNECT ();
         int DP_MSG_HOTLINK ();
         int DP_MSG_VALUECHANGE ();
         int DP_MSG_DRIVER_VC ();
         int DP_MSG_COMPLEX_VC ();
         int DP_MSG_REQUEST ();
         int DP_MSG_ANSWER ();
         int DP_MSG_SIMPLE_REQUEST ();
         int DP_MSG_ASYNCH_REQUEST ();
         int DP_MSG_SYNCH_REQUEST ();
         int DP_MSG_PERIOD_REQUEST ();
         int DP_MSG_ALERT_VC ();
         int DP_MSG_ALERT_CONNECT ();
         int DP_MSG_ALERT_CONNECT_VISIBLE ();
         int DP_MSG_ALERT_DISCONNECT ();
         int DP_MSG_ALERT_HL ();
         int DP_MSG_ALERT_TIME_REQUEST ();
         int DP_MSG_ALERT_PERIOD_REQUEST ();
         int DP_MSG_FILTER_REQUEST ();
         int DP_MSG_FILTER_CONNECT ();
         int DP_MSG_FILTER_DISCONNECT ();
         int DP_MSG_FILTER_HL ();
         int DP_MSG_ALERT ();
         int DP_MSG_DISCONNECT_ALL ();
         int DP_MSG_ABORT_REQUEST ();
         int DP_MSG_CNS_REQ ();
         int DP_MSG_MANIP_CNS ();
         int DP_MSG_MAXAGE_REQUEST ();
    }

    public static class MsgTypeV3 implements MsgType {
        public int NO_MSG() { return 1; }
        public int SYS_MSG() { return 2; }
        public int SYS_MSG_START_DPINIT() { return 3; }
        public int SYS_MSG_NAMESERVER() { return 4; }
        public int SYS_MSG_LICENSE() { return 5; }
        public int SYS_MSG_INIT() { return 6; }
        public int SYS_MSG_FILE_TRANSFER() { return 7; }
        public int DP_MSG() { return 8; }
        public int DP_MSG_INITCONFIG() { return 9; }
        public int DP_MSG_IDENTIFICATION() { return 10; }
        public int DP_MSG_TYPECONTAINER() { return 11; }
        public int DP_MSG_DPTYPE_REQ() { return 12; }
        public int DP_MSG_MANIP_DPTYPE() { return 13; }
        public int DP_MSG_DP_REQ() { return 14; }
        public int DP_MSG_MANIP_DP() { return 15; }
        public int DP_MSG_DPALIAS_REQ() { return 16; }
        public int DP_MSG_MANIP_DPALIAS() { return 17; }
        public int DP_MSG_CONNECTION() { return 18; }
        public int DP_MSG_CONNECT() { return 19; }
        public int DP_MSG_CONNECT_RET() { return 20; }
        public int DP_MSG_CONNECT_NOSOURCE() { return 21; }
        public int DP_MSG_DISCONNECT() { return 22; }
        public int DP_MSG_HOTLINK() { return 23; }
        public int DP_MSG_VALUECHANGE() { return 24; }
        public int DP_MSG_DRIVER_VC() { return 25; }
        public int DP_MSG_COMPLEX_VC() { return 26; }
        public int DP_MSG_REQUEST() { return 27; }
        public int DP_MSG_ANSWER() { return 28; }
        public int DP_MSG_SIMPLE_REQUEST() { return 29; }
        public int DP_MSG_ASYNCH_REQUEST() { return 30; }
        public int DP_MSG_SYNCH_REQUEST() { return 31; }
        public int DP_MSG_PERIOD_REQUEST() { return 32; }
        public int DP_MSG_ALERT_VC() { return 33; }
        public int DP_MSG_ALERT_CONNECT() { return 34; }
        public int DP_MSG_ALERT_CONNECT_VISIBLE() { return 35; }
        public int DP_MSG_ALERT_DISCONNECT() { return 36; }
        public int DP_MSG_ALERT_HL() { return 37; }
        public int DP_MSG_ALERT_TIME_REQUEST() { return 38; }
        public int DP_MSG_ALERT_PERIOD_REQUEST() { return 39; }
        public int DP_MSG_FILTER_REQUEST() { return 40; }
        public int DP_MSG_FILTER_CONNECT() { return 41; }
        public int DP_MSG_FILTER_DISCONNECT() { return 42; }
        public int DP_MSG_FILTER_HL() { return 43; }
        public int DP_MSG_ALERT() { return 44; }
        public int DP_MSG_DISCONNECT_ALL() { return 45; }
        public int DP_MSG_ABORT_REQUEST() { return 46; }
        public int DP_MSG_CNS_REQ() { return 47; }
        public int DP_MSG_MANIP_CNS() { return 48; }
        public int DP_MSG_MAXAGE_REQUEST() { return 50; }
    }

    public static class MsgTypeV4 implements MsgType {
        public int NO_MSG() { return  0; }
        public int SYS_MSG() { return  1; }
        public int SYS_MSG_START_DPINIT() { return  2; }
        public int SYS_MSG_NAMESERVER() { return  3; }
        public int SYS_MSG_INIT() { return  4; }
        public int SYS_MSG_FILE_TRANSFER() { return  5; }
        public int DP_MSG() { return  6; }
        public int DP_MSG_INITCONFIG() { return  7; }
        public int DP_MSG_IDENTIFICATION() { return  8; }
        public int DP_MSG_TYPECONTAINER() { return  9; }
        public int DP_MSG_DPTYPE_REQ() { return  10; }
        public int DP_MSG_MANIP_DPTYPE() { return  11; }
        public int DP_MSG_DP_REQ() { return  12; }
        public int DP_MSG_MANIP_DP() { return  13; }
        public int DP_MSG_DPALIAS_REQ() { return  14; }
        public int DP_MSG_MANIP_DPALIAS() { return  15; }
        public int DP_MSG_CONNECTION() { return  16; }
        public int DP_MSG_CONNECT() { return  17; }
        public int DP_MSG_CONNECT_RET() { return  18; }
        public int DP_MSG_CONNECT_NOSOURCE() { return  19; }
        public int DP_MSG_DISCONNECT() { return  20; }
        public int DP_MSG_HOTLINK() { return  21; }
        public int DP_MSG_VALUECHANGE() { return  22; }
        public int DP_MSG_DRIVER_VC() { return  23; }
        public int DP_MSG_COMPLEX_VC() { return  24; }
        public int DP_MSG_REQUEST() { return  25; }
        public int DP_MSG_ANSWER() { return  26; }
        public int DP_MSG_SIMPLE_REQUEST() { return  27; }
        public int DP_MSG_ASYNCH_REQUEST() { return  28; }
        public int DP_MSG_SYNCH_REQUEST() { return  29; }
        public int DP_MSG_PERIOD_REQUEST() { return  30; }
        public int DP_MSG_ALERT_VC() { return  31; }
        public int DP_MSG_ALERT_CONNECT() { return  32; }
        public int DP_MSG_ALERT_CONNECT_VISIBLE() { return  33; }
        public int DP_MSG_ALERT_DISCONNECT() { return  34; }
        public int DP_MSG_ALERT_HL() { return  35; }
        public int DP_MSG_ALERT_TIME_REQUEST() { return  36; }
        public int DP_MSG_ALERT_PERIOD_REQUEST() { return  37; }
        public int DP_MSG_FILTER_REQUEST() { return  38; }
        public int DP_MSG_FILTER_CONNECT() { return  39; }
        public int DP_MSG_FILTER_DISCONNECT() { return  40; }
        public int DP_MSG_FILTER_HL() { return  41; }
        public int DP_MSG_ALERT() { return  42; }
        public int DP_MSG_DISCONNECT_ALL() { return  43; }
        public int DP_MSG_ABORT_REQUEST() { return  44; }
        public int DP_MSG_CNS_REQ() { return  45; }
        public int DP_MSG_MANIP_CNS() { return  46; }
        public int DP_MSG_MAXAGE_REQUEST() { return  47; }
        public int SERVICE_MSG() { return  48; }
        public int SERVICE_MSG_ANSWER() { return  49; }
        public int DP_MSG_CONNECT_EXT() { return  50; }
        public int DP_MSG_CONNECT_EXT_NOSOURCE() { return  51; }
        public int DP_MSG_DISCONNECT_EXT() { return  52; }
        public int DP_MSG_CHROMTYPECONTAINER() { return  53; }
        public int DP_MSG_CHROM_TYPE_REQ() { return  54; }
        public int DP_MSG_MANIP_CHROM_TYPE() { return  55; }
        public int DP_MSG_CHROMIDENTIFICATION() { return  56; }
        public int DP_MSG_CHROM_NAME_REQ() { return  57; }
        public int DP_MSG_MANIP_CHROM_NAME() { return  58; }
        public int SYS_MSG_CAL() { return  59; }
        public int SYS_MSG_DELTA() { return  60; }
        public int DP_MSG_CONNECT_ALT_EXT() { return  61; }
        public int DP_MSG_CONNECT_ALT_EXT_NOSOURCE() { return  62; }
        public int DP_MSG_DISCONNECT_ALT_EXT() { return  63; }
        public int DP_MSG_DP_NAME_REQ() { return  64; }
        public int DP_MSG_MANIP_DP_NAME() { return  65; }
        public int DP_MSG_NAME_SERVICE_INIT() { return  66; }
        public int DP_MSG_MANIP() { return  67; }
        public int DP_MSG_MANIP_REQUEST() { return  68; }
        public int DP_MSG_MANIP_NOTIFY() { return  69; }
        public int DP_MSG_MANIP_END_OF_CHANGE_UNIT_REQ() { return  70; }
        public int DP_MSG_MANIP_END_OF_CHANGE_UNIT() { return  71; }
        public int DP_MSG_ENUMERATE_REQUEST() { return  72; }
        public int SYS_MSG_MANAGER_DISPATCH() { return  73; }
        public int DP_MSG_SYSTEM_IDENTIFICATION() { return  74; }
        public int DP_MSG_CNS() { return  75; }
        public int NAME_MSG() { return  76; }
        public int NAME_MSG_REQUEST() { return  77; }
        public int NAME_MSG_ANSWER() { return  78; }
        public int LAST_MSG_TYPE() { return  79; }
    }
}
