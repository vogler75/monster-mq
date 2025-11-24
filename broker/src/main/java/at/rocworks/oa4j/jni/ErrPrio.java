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
 * WinCC OA error priority levels (ErrClass::ErrPrio)
 */
public enum ErrPrio {
    /** Fatal error. Kill the program instance! */
    PRIO_FATAL(0),
    /** Severe error, but we can continue */
    PRIO_SEVERE(1),
    /** Warning message, something is not as it should be */
    PRIO_WARNING(2),
    /** Info message */
    PRIO_INFO(3);

    private final int value;

    ErrPrio(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
