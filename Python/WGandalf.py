#!/usr/bin/env python
# -*- coding: latin-1 -*-
#
# Copyright (c) Priit Järv 2009, 2010
#
# This file is part of wgandalf
#
# Wgandalf is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# Wgandalf is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with Wgandalf.  If not, see <http://www.gnu.org/licenses/>.

"""@file WGandalf.py

High level Python API for WGandalf database
"""

# This module is implemented loosely following the guidelines
# in Python DBI spec (http://www.python.org/dev/peps/pep-0249/).
# Due to the wgdb feature set being much slimmer than typical
# SQL databases, it does not make sense to follow DBI fully,
# but where there are overlaps in functionality, similar
# naming and object structure should be generally used.

import wgdb

### Error classes (by DBI recommendation) ###
#

class DatabaseError(wgdb.error):
    """Base class for database errors"""
    pass

class ProgrammingError(DatabaseError):
    """Exception class to indicate invalid database usage"""
    pass

class DataError(DatabaseError):
    """Exception class to indicate invalid data passed to the db adapter"""
    pass

class InternalError(DatabaseError):
    """Exception class to indicate invalid internal state of the module"""
    pass


##############  DBI classes: ###############
#

class Connection:
    """The Connection class acts as a container for
wgdb.Database and provides all connection-related
and record accessing functions."""
    def __init__(self, shmname=None, shmsize=0, local=0):
        if local:
            self._db = wgdb.attach_database(size=shmsize, local=local)
        elif shmname:
            self._db = wgdb.attach_database(shmname, shmsize)
        else:
            self._db = wgdb.attach_database(size=shmsize)
        self.shmname = shmname
        self.locking = 1
        self._lock_id = None

    def close(self):
        """Close the connection."""
# XXX: keep the database attached so that when garbage collection
# goes through query objects their shared memory can be freed normally.
#        if self._db:
#            wgdb.detach_database(self._db)
        self._db = None

    def cursor(self):
        """Return a DBI-style database cursor"""
        if self._db is None:
            raise InternalError, "Connection is closed."
        return Cursor(self)

    # Locking support
    #
    def set_locking(self, mode):
        """Set locking mode (1=on, 0=off)"""
        self.locking = mode

    def start_write(self):
        """Start writing transaction"""
        if self._lock_id:
            raise ProgrammingError, "Transaction already started."
        self._lock_id = wgdb.start_write(self._db)

    def end_write(self):
        """Finish writing transaction"""
        if not self._lock_id:
            raise ProgrammingError, "No current transaction."
        wgdb.end_write(self._db, self._lock_id)
        self._lock_id = None

    def start_read(self):
        """Start reading transaction"""
        if self._lock_id:
            raise ProgrammingError, "Transaction already started."
        self._lock_id = wgdb.start_read(self._db)

    def end_read(self):
        """Finish reading transaction"""
        if not self._lock_id:
            raise ProgrammingError, "No current transaction."
        wgdb.end_read(self._db, self._lock_id)
        self._lock_id = None

    def commit(self):
        """Commit the transaction (no-op)"""
        pass

    def rollback(self):
        """Roll back the transaction (no-op)"""
        pass

    # Record operations. Wrap wgdb.Record object into Record class.
    #
    def _new_record(self, rec):
        """Create a Record instance from wgdb record object (internal)"""
        r = Record(self, rec)
        if self.locking:
            self.start_read()
        try:
            r.size = wgdb.get_record_len(self._db, rec)
        finally:
            if self.locking:
                self.end_read()
        return r
        
    def first_record(self):
        """Get first record from database."""
        if self.locking:
            self.start_read()
        try:
            r = wgdb.get_first_record(self._db)
        except wgdb.error:
            r = None
        finally:
            if self.locking:
                self.end_read()

        if not r:
            return None
        return self._new_record(r)
    
    def next_record(self, rec):
        """Get next record from database."""
        if self.locking:
            self.start_read()
        try:
            r = wgdb.get_next_record(self._db, rec.get__rec())
        except wgdb.error:
            r = None
        finally:
            if self.locking:
                self.end_read()

        if not r:
            return None
        return self._new_record(r)
        
    def create_record(self, size):
        """Create new record with given size."""
        if self.locking:
            self.start_write()
        try:
            r = wgdb.create_record(self._db, size)
        finally:
            if self.locking:
                self.end_write()
        return self._new_record(r)

    def delete_record(self, rec):
        """Delete record."""
        if self.locking:
            self.start_write()
        try:
            r = wgdb.delete_record(self._db, rec.get__rec())
        finally:
            if self.locking:
                self.end_write()
        rec.set__rec(None)  # prevent future usage

    def atomic_create_record(self, fields):
        """Create a record and set field contents atomically."""
        if not fields:
            raise DataError, "Cannot create an empty record"
        l = len(fields)
        tupletype = type(())

        if self.locking:
            self.start_write()
        try:
            r = wgdb.create_raw_record(self._db, l)
            for i in range(l):
                if type(fields[i]) == tupletype:
                    data = fields[i][0]
                    extarg = fields[i][1:]
                else:
                    data = fields[i]
                    extarg = ()
                if isinstance(data, Record):
                    data = data.get__rec()
                fargs = (self._db, r, i, data) + extarg
                wgdb.set_new_field(*fargs)
        finally:
            if self.locking:
                self.end_write()

        return self._new_record(r)

    def atomic_update_record(self, rec, fields):
        """Set the contents of the entire record atomically."""
        # fields should be a sequence
        l = len(fields)
        sz = rec.get_size()
        r = rec.get__rec()
        tupletype = type(())

        if self.locking:
            self.start_write()
        try:
            for i in range(l):
                if type(fields[i]) == tupletype:
                    data = fields[i][0]
                    extarg = fields[i][1:]
                else:
                    data = fields[i]
                    extarg = ()
                if isinstance(data, Record):
                    data = data.get__rec()
                fargs = (self._db, r, i, data) + extarg
                wgdb.set_field(*fargs)

            if l < sz:
                # fill the remainder:
                for i in range(l, sz):
                    wgdb.set_field(self._db, r, i, None)
        finally:
            if self.locking:
                self.end_write()

    # alias for atomic_create_record()
    def insert(self, fields):
        """Insert a record into database"""
        return self.atomic_create_record(fields)

    # Field operations. Expect Record instances as argument
    #
    def get_field(self, rec, fieldnr):
        """Return data field contents"""
        if self.locking:
            self.start_read()
        try:
            data = wgdb.get_field(self._db, rec.get__rec(), fieldnr)
        finally:
            if self.locking:
                self.end_read()

        if wgdb.is_record(data):
            return self._new_record(data)
        else:
            return data

    def set_field(self, rec, fieldnr, data, *arg, **kwarg):
        """Set data field contents"""
        if isinstance(data, Record):
            data = data.get__rec()

        if self.locking:
            self.start_write()
        try:
            r = wgdb.set_field(self._db,
                rec.get__rec(), fieldnr, data, *arg, **kwarg)
        finally:
            if self.locking:
                self.end_write()
        return r

class Cursor:
    """Pseudo-cursor object. Since there are no queries
available, this allows fetching from the set of all records
and inserting records."""
    def __init__(self, conn):
        self._curr = None
        self._conn = conn

    def fetchone(self):
        """Fetch the next record from database"""
        if self._curr is None:
            r = self._conn.first_record()
        else:
            r = self._conn.next_record(self._curr)
        if r:
            self._curr = r
            return self._curr.fetch()

    def fetchall(self):
        """Fetch all (remaining) records from database"""
        result = []
        while 1:
            r = self.fetchone()
            if not r: break
            result.append(r)
        return result

    # using cursors to insert data does not make sense
    # in WGandalf context, since there is no relation at all
    # between the current cursor state and new records.
    # This functionality will be moved to Connection object.
    def insert(self, fields):
        """Insert a record into database --DEPRECATED--"""
        return self._conn.atomic_create_record(fields)

    def close(self):
        """Close the cursor"""
        pass

##############  Additional classes: ###############
#

class Record:
    """Record data representation. Allows field-level and record-level
manipulation of data."""
    def __init__(self, conn, rec):
        self._rec = rec
        self._conn = conn
        self.size = 0

    def get__rec(self):
        """Return low level record object"""
        return self._rec

    def set__rec(self, rec):
        """Overwite low level record object"""
        self._rec = rec

    def get_size(self):
        """Return record size"""
        return self.size

    def get_field(self, fieldnr):
        """Return data field contents"""
        if fieldnr < 0 or fieldnr >= self.size:
            raise DataError, "Field number out of bounds."
        return self._conn.get_field(self, fieldnr)

    def set_field(self, fieldnr, data, *arg, **kwarg):
        """Set data field contents with optional encoding"""
        if fieldnr < 0 or fieldnr >= self.size:
            raise DataError, "Field number out of bounds."
        return self._conn.set_field(self, fieldnr, data, *arg, **kwarg)

    def fetch(self):
        """Return the contents of the record as tuple"""
        result = []
        for i in range(self.size):
            result.append(self.get_field(i))
        return tuple(result)
        
    def update(self, fields):
        """Set the contents of the entire record"""
        self._conn.atomic_update_record(self, fields)

    def delete(self):
        """Delete the record from database"""
        self._conn.delete_record(self)

##############  DBI API functions: ###############
#

def connect(shmname=None, shmsize=0, local=0):
    """Attaches to (or creates) a database. Returns a database object"""
    return Connection(shmname, shmsize, local)
