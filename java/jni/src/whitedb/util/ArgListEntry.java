/*
* $Id:  $
* $Version: $
*
* Copyright (c) Andres Puusepp 2009
*
* This file is part of WhiteDB
*
* WhiteDB is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
* 
* WhiteDB is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
* 
* You should have received a copy of the GNU General Public License
* along with WhiteDB.  If not, see <http://www.gnu.org/licenses/>.
*
*/

 /** @file FieldComparator.java
 *  WhiteDB Java API ORM support code.
 *
 */

package whitedb.util;

/* XXX: currently only allows int arguments */
public class ArgListEntry {
    public int column;
    public int cond;
    public int value;

    public ArgListEntry(int column, int cond, int value) {
        this.column = column;
        this.cond = cond;
        this.value = value;
    }
}
