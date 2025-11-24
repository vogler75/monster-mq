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
public enum JDpAttrAddrDirection {
    UNDEFINED(0),
    OUTPUT(1),
    INPUT_SPONT(2),
    INPUT_SQUERY(3),
    INPUT_POLL(4),
    OUTPUT_SINGLE(5),
    IO_SPONT(6),
    IO_POLL(7),
    IO_SQUERY(8),
    AM_ALERT(9),
    INPUT_ON_DEMAND(10),
    INPUT_CYCLIC_ON_USE(11),
    IO_ON_DEMAND(12),
    IO_CYCLIC_ON_USE(13),
    INTERNAL(32),
    LOW_LEVEL_FLAG(64);

    JDpAttrAddrDirection(int n) {
        value = n;
    }

    public final int value;
}
