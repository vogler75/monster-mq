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
 * WinCC OA error codes (ErrClass error codes)
 */
public enum ErrCode {
    /** No error */
    NOERR(0),
    /** Manager is starting */
    MANAGER_START(1),
    /** Manager is stopping */
    MANAGER_STOP(2),
    /** Manager is trying to connect */
    TRYING_CONNECT(3),
    /** Manager is connected */
    CONNECTED(4),
    /** Connect failed */
    CANT_CONNECT(5),
    /** Manager is initialized, switched to STATE_RUNNING */
    MANAGER_INITIALIZED(6),
    /** Datapoint not found */
    DPNOTEXISTENT(7),
    /** Datapoint element not found */
    ELNOTEXISTENT(8),
    /** Datapoint config not found */
    CONFIGNOTEXISTENT(9),
    /** Wrong attribute address */
    WRONGATTRIBADRESS(10),
    /** Data conversion not possible - incompatible datatypes */
    ILLEGALCONVERSION(11),
    /** Smoothing error */
    SMOOTHINGERR(12),
    /** Error on conversion to engineering value */
    CONVERSION2ING(13),
    /** Error on conversion to raw value */
    CONVERSION2RAW(14),
    /** No periphery configuration found */
    MISSINGCONFIG(15),
    /** This module is unknown */
    MISSINGMODULE(16),
    /** This panel is unknown */
    MISSINGPANEL(17),
    /** Value out of alert ranges */
    OUTOFALERTRANGES(18),
    /** Attribute not existent */
    ATTRIBUTENOTEXISTENT(19),
    /** Origin time out of valid range */
    ORIGINTIMEINCORRECT(20),
    /** Wrong config type for this dp element */
    CONFIGTYPEINCORRECT(21),
    /** Internal check error - config not created */
    NOCONSISTENCE(22),
    /** Attribute not set / changed */
    CHANGEATTRIBUTEFAILED(23),
    /** This config is locked by someone else */
    CONFIGLOCKED(24),
    /** This config is not locked */
    CONFIGISNOTLOCKED(25),
    /** This config is not locked by you */
    OTHERUSERLOCKED(26),
    /** Wrong variable type for lock config */
    INCORRECTLOCKVARIABLE(27),
    /** Creation of config failed */
    CREATECONFIGFAILED(28),
    /** Value out of PVSS-II range */
    OUTOFPVSSRANGE(29),
    /** Value out of user range */
    OUTOFUSERRANGE(30),
    /** Config type cannot be changed */
    TYPECHANGEFAILED(31),
    /** Connection not existent */
    CONNECTIONNOTEXISTENT(32),
    /** Common error in data manager */
    DM(33),
    /** This datapoint already exists */
    DPEXISTS(34),
    /** This datapoint type already exists */
    TYPEEXISTS(35),
    /** Check the spelling of this name */
    SYNTAX(36),
    /** Concurrent access tried on object */
    CONCURRENT(37),
    /** Message send error */
    MSGSENDERROR(38),
    /** Connection broken */
    BROKENCONNECTION(39),
    /** Parent panel missing */
    MISSINGPARENTPANEL(40),
    /** This datapoint element is inactive */
    DPVARIABLEINACTIVE(41),
    /** No default value defined */
    NODEFAULTVALUE(42),
    /** New origin time is older than last change */
    ORIGINTIMEOLDER(43),
    /** Alert class has changed */
    ALERTPRIORITY(44),
    /** This is no valid control script */
    CTRL(45),
    /** No archived values available */
    NOTARCHIVED(46),
    /** Duplicate peripheral address for output channel */
    DUPLICATEOUTPUTPA(47),
    /** Value not corrected */
    NOTCORRECTED(48),
    /** Write failed due to wrong address */
    NOTEXISTINGADDRESS(49),
    /** Default branch called */
    DEFAULTBRANCH(50),
    /** Parameter error */
    PARAMETERERROR(51),
    /** Wrong message type */
    WRONGMSGTYPE(52),
    /** Illegal function call */
    ILLEGALFUNCALL(53),
    /** Unexpected state */
    UNEXPECTEDSTATE(54),
    /** Wrong error description type */
    WRONGERRDESCR(55),
    /** Invalid system number for datapoint */
    INVALIDSYSNUM(56),
    /** Unknown datapoint type */
    TYPENOTEXISTENT(57),
    /** Datapoint has not been allocated */
    DPALLOCFAIL(58),
    /** Unknown datapoint element */
    ELEMENTNOTEXISTENT(59),
    /** Config allocation failed */
    CONFIGALLOCFAIL(60),
    /** Cannot open file */
    FILEOPEN(61),
    /** Referred alert class deleted */
    USEDALERTCLASSFREED(62),
    /** Error on manager connection */
    CONNECTIONFAILURE(63),
    /** Wrong IPC type */
    WRONGIPCTYPE(64),
    /** Wrong destination */
    WRONGDESTINATION(65),
    /** Error on connection startup */
    STARTCONNECT(66),
    /** Error on sending message */
    SENDFAILURE(67),
    /** Process died */
    PROCESSDIED(68);

    private final long value;

    ErrCode(long value) {
        this.value = value;
    }

    public long getValue() {
        return value;
    }
}
