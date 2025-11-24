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
public enum ManagerState {
    /** (0). The manager has been started and is trying to connect to DM */
    STATE_JUST_STARTED(0),
    /** (1). The manager has connection to DM and is receiving init data */
    STATE_INIT(1),
    /** (2). Init from database finished. Manager is now ready to finish
      manager specific initialisation and to connect to event.
     */
    STATE_ADJUST(2),
    /** (3). Connection to EM established. The manager is now ready for operation
     */
    STATE_RUNNING(3);

    ManagerState(int value) {
        this.value = value;
    }
    public int value;       
}
