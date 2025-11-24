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
public enum ManagerType {
    NO_MAN(0),
    EVENT_MAN(1),
    DRIVER_MAN(2),
    DB_MAN(3),
    UI_MAN(4),
    CTRL_MAN(5),
    ASCII_MAN(6),
    API_MAN(7),
    DEVICE_MAN(8),
    REDU_MAN(9),
    REPORT_MAN(10),
    DDE_MAN(11),
    DIST_MAN(12);

    ManagerType(int value) {
        this.value = value;
    }
    public final int value;
}
