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
public enum SysMsgRedundancySubType {
    NONE(0),

    REDUNDANCY_ACTIVE(1),     // Manager goes active
    REDUNDANCY_PASSIVE(2),        // Manager goes passive
    REDUNDANCY_REFRESH(3),        // Tell manager to refresh the connects to origSource
    REDUNDANCY_DISCONNECT(4),     // Tell event to clear all connects to origSource
    DM_START_TOUCHING(5),         // Tell data to start touching
    DM_STOP_TOUCHING(6),          // Tell data to stop touching
    REDUNDANCY_ACTIVE_REQ(7),     // Initiate Redu-State-Switch
    REDUNDANCY_PASSIVE_REQ(8),     // Initiate Redu-State-Switch
    REDUNDANCY_PEER_RECONN(9);          // Redundancy peer reconnected

    SysMsgRedundancySubType(int value) {
        this.value = value;
    }
    public int value;
}
