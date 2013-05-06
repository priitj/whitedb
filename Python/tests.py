#!/usr/bin/env python
# -*- coding: latin-1 -*-
#
# Copyright (c) Priit Järv 2012,2013
#
# This file is part of WhiteDB
#
# WhiteDB is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# WhiteDB is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with WhiteDB.  If not, see <http://www.gnu.org/licenses/>.

"""@file tests.py

Unit tests for the WhiteDB Python API
"""

import unittest

import wgdb
import whitedb

import datetime

MINDBSIZE=8000000 # should cover 64-bit databases that need more memory

class LowLevelTest(unittest.TestCase):
    """Provide setUp()/tearDown() for test cases that
    use the low level Python API."""

    def setUp(self):
        self.d = wgdb.attach_database(size=MINDBSIZE, local=1)

    def tearDown(self):
        wgdb.detach_database(self.d)

class RecordTests(LowLevelTest):
    """Test low level record functionality"""

    def test_creation(self):
        """Tests record creation and low level
        scanning to retrieve records from the database."""

        rec = wgdb.create_record(self.d, 3)
        self.assertTrue(wgdb.is_record(rec))
        l = wgdb.get_record_len(self.d, rec)
        self.assertEqual(l, 3)
        rec2 = wgdb.create_raw_record(self.d, 678)
        self.assertTrue(wgdb.is_record(rec2))
        l = wgdb.get_record_len(self.d, rec2)
        self.assertEqual(l, 678)
        
        # wgdb module only allows comparing records by contents
        # so we need to use recognizable data for this test.
        wgdb.set_field(self.d, rec, 0, 99531179)
        wgdb.set_field(self.d, rec2, 0, 55498756)

        # XXX: the following relies on certain assumptions on memory
        # management of WhiteDB. By the API description, the records
        # are not necessarily fetched in order of creation, it is just
        # useful for the current test case that it happens to be the case.
        #
        cand = wgdb.get_first_record(self.d)
        self.assertEqual(wgdb.get_field(self.d, cand, 0), 99531179)
        cand = wgdb.get_next_record(self.d, cand)
        self.assertEqual(wgdb.get_field(self.d, cand, 0), 55498756)

        # This, however, should always work correctly
        wgdb.delete_record(self.d, rec)
        cand = wgdb.get_first_record(self.d)
        self.assertEqual(wgdb.get_field(self.d, cand, 0), 55498756)

    def test_field_data(self):
        """Tests field data encoding and decoding."""

        # BLOBTYPE not supported yet
        #blob = "\045\120\104\106\055\061\056\065\012\045"\
        #    "\265\355\256\373\012\063\040\060\040\157\142\152\012\074\074"\
        #    "\040\057\114\145\156\147\164\150\040\064\040\060\040\122\012"\
        #    "\040\040\040\057\106\151\154\164\145\162\040\057\106\154\141"\
        #    "\164\145\104\145\143\157\144\145\012\076\076\012\163\164\162"\
        #    "\145\141\155\012\170\234\255\227\333\152\334\060\020\100\337"\
        #    "\375\025\372\001\357\152\106\067\013\312\076\024\112\041\120"\
        #    "\150\132\103\037\102\036\366\032\010\064\064\027\350\357\167"\
        #    "\106\222\327\326\156\222\125\152\141\144\153\155\315\350\150"\
        #    "\146\064\243\175\154\224\025\316\130\141\264\024\255\103\051"\
        #    "\236\366\342\227\170\150\100\360\365\164\047\226\153\051\356"
        s1 = "Qly9y63M84Qly9y63M84Qly9y63M84Qly9y63M84Qly9y63M84Qly9y63M84"
        s2 = "2O15At13Iu"
        s3 = "A Test String"
        s4 = "#testobject"
        s5 = "http://example.com/testns"
        s6 = "9091270"
        s7 = "xsd:integer"

        rec = wgdb.create_record(self.d, 16)

        # BLOBTYPE not supported yet
        #wgdb.set_field(self.d, rec, 0, blob, wgdb.BLOBTYPE, "blob.pdf")
        #val = wgdb.get_field(self.d, rec, 0)
        #self.assertEqual(type(val), type(()))
        #self.assertEqual(len(val), 3)
        #self.assertEqual(val[0], blob)
        #self.assertEqual(val[1], wgdb.BLOBTYPE)
        #self.assertEqual(val[2], "blob.pdf")
        
        # CHARTYPE
        wgdb.set_field(self.d, rec, 1, "c", wgdb.CHARTYPE)
        val = wgdb.get_field(self.d, rec, 1)
        self.assertEqual(val, "c")

        # DATETYPE
        wgdb.set_field(self.d, rec, 2, datetime.date(2040, 7, 24))
        val = wgdb.get_field(self.d, rec, 2)
        self.assertTrue(isinstance(val, datetime.date))
        self.assertEqual(val.day, 24)
        self.assertEqual(val.month, 7)
        self.assertEqual(val.year, 2040)

        # DOUBLETYPE
        wgdb.set_field(self.d, rec, 3, -0.94794830)
        val = wgdb.get_field(self.d, rec, 3)
        self.assertAlmostEqual(val, -0.94794830)

        # FIXPOINTTYPE
        wgdb.set_field(self.d, rec, 4, 549.8390, wgdb.FIXPOINTTYPE)
        val = wgdb.get_field(self.d, rec, 4)
        self.assertEqual(val, 549.8390)

        # INTTYPE
        wgdb.set_field(self.d, rec, 5, 2073741877)
        val = wgdb.get_field(self.d, rec, 5)
        self.assertEqual(val, 2073741877)
        wgdb.set_field(self.d, rec, 6, -10)
        val = wgdb.get_field(self.d, rec, 6)
        self.assertEqual(val, -10)

        # NULLTYPE
        wgdb.set_field(self.d, rec, 7, None)
        val = wgdb.get_field(self.d, rec, 7)
        self.assertIsNone(val)

        # RECORDTYPE
        rec2 = wgdb.create_record(self.d, 1)
        wgdb.set_field(self.d, rec, 8, rec2)
        wgdb.set_field(self.d, rec2, 0, 30755904)
        val = wgdb.get_field(self.d, rec, 8)
        self.assertTrue(wgdb.is_record(val))
        self.assertEqual(wgdb.get_field(self.d, val, 0), 30755904)

        # STRTYPE
        wgdb.set_field(self.d, rec, 9, s1)
        val = wgdb.get_field(self.d, rec, 9)
        self.assertEqual(val, s1)
        wgdb.set_field(self.d, rec, 10, s2, wgdb.STRTYPE)
        val = wgdb.get_field(self.d, rec, 10)
        self.assertEqual(val, s2)
        # extra string not supported yet
        #wgdb.set_field(self.d, rec, 11, s3, ext_str="en")
        #val = wgdb.get_field(self.d, rec, 11)
        #self.assertEqual(val, s3)

        # TIMETYPE
        wgdb.set_field(self.d, rec, 12, datetime.time(23, 44, 6))
        val = wgdb.get_field(self.d, rec, 12)
        self.assertTrue(isinstance(val, datetime.time))
        self.assertEqual(val.hour, 23)
        self.assertEqual(val.minute, 44)
        self.assertEqual(val.second, 6)

        # URITYPE
        wgdb.set_field(self.d, rec, 13, s4, wgdb.URITYPE, s5)
        val = wgdb.get_field(self.d, rec, 13)
        self.assertEqual(val, s5 + s4)

        # XMLLITERALTYPE
        wgdb.set_field(self.d, rec, 14, s6, wgdb.XMLLITERALTYPE, s7)
        val = wgdb.get_field(self.d, rec, 14)
        self.assertEqual(val, s6)

        # VARTYPE
        # when decoded, a tuple is returned that contains the
        # value and database (kind of a representation of vartype).
        wgdb.set_field(self.d, rec, 15, 2, wgdb.VARTYPE)
        val = wgdb.get_field(self.d, rec, 15)
        self.assertEqual(type(val), type(()))
        self.assertEqual(len(val), 2)
        self.assertEqual(val[0], 2)
        self.assertEqual(val[1], wgdb.VARTYPE)

class LowLevelQueryTest(LowLevelTest):
    """Helper functions for query testing"""

    def fetch(self, query):
        try:
            rec = wgdb.fetch(self.d, query)
        except wgdb.error:
            rec = None
        return rec

    def get_first_record(self):
        try:
            rec = wgdb.get_first_record(self.d)
        except wgdb.error:
            rec = None
        return rec

    def get_next_record(self, rec):
        try:
            rec = wgdb.get_next_record(self.d, rec)
        except wgdb.error:
            rec = None
        return rec

class QueryTests(LowLevelQueryTest):
    """Test low level query functions"""

    def make_testdata(self, dbsize):
        """Generates patterned test data for the query."""

        for i in range(dbsize):
            for j in range(50):
                for k in range(50):
                    rec = wgdb.create_record(self.d, 3)
                    c1 = str(10 * i)
                    c2 = 100 * j
                    c3 = float(1000 * k)
                    wgdb.set_field(self.d, rec, 0, c1)
                    wgdb.set_field(self.d, rec, 1, c2)
                    wgdb.set_field(self.d, rec, 2, c3) 

    def check_matching_rows(self, col, cond, val, expected):
        """Fetch all rows where "col" "cond" "val" is true
            (where cond is a comparison operator - equal, less than etc)
        Check that the val matches the field value in returned records.
        Check that the number of rows matches the expected value"""

        query = wgdb.make_query(self.d, arglist = [(col, cond, val)])

        # XXX: should check rowcount here when it's implemented
        # self.assertEqual(expected, query rowcount)

        cnt = 0
        rec = self.fetch(query)
        while rec is not None:
            dbval = wgdb.get_field(self.d, rec, col)
            self.assertEqual(type(val), type(dbval))
            self.assertEqual(val, dbval)
            cnt += 1
            rec = self.fetch(query)

        self.assertEqual(cnt, expected)

    def check_db_rows(self, expected):
        """Count db rows."""

        cnt = 0
        rec = self.get_first_record()
        while rec is not None:
            cnt += 1
            rec = self.get_next_record(rec)

        self.assertEqual(cnt, expected)

    def test_query(self):
        """Tests various queries:
            - read pre-generated content;
            - update content;
            - read updated content;
            - delete rows;
            - check row count after deleting.
        """

        dbsize = 10 # use a fairly small database
        self.make_testdata(dbsize)

        # Content check read queries
        for i in range(dbsize):
            val = str(10 * i)
            self.check_matching_rows(0, wgdb.COND_EQUAL, val, 50*50)

        for i in range(50):
            val = 100 * i
            self.check_matching_rows(1, wgdb.COND_EQUAL, val, dbsize*50)

        for i in range(50):
            val = float(1000 * i)
            self.check_matching_rows(2, wgdb.COND_EQUAL, val, dbsize*50)

        # Update queries
        for i in range(dbsize):
            c1 = str(10 * i)

            query = wgdb.make_query(self.d,
                arglist = [(0, wgdb.COND_EQUAL, c1)])
            rec = self.fetch(query)
            while rec is not None:
                c2 = wgdb.get_field(self.d, rec, 1)
                wgdb.set_field(self.d, rec, 1, c2 - 34555)
                rec = self.fetch(query)

        for i in range(50):
            c2 = 100 * i - 34555

            query = wgdb.make_query(self.d,
                arglist = [(1, wgdb.COND_EQUAL, c2)])
            rec = self.fetch(query)
            while rec is not None:
                c3 = wgdb.get_field(self.d, rec, 2)
                wgdb.set_field(self.d, rec, 2, c3 + 177889.576)
                rec = self.fetch(query)

        for i in range(50):
            c3 = 1000 * i + 177889.576

            query = wgdb.make_query(self.d,
                arglist = [(2, wgdb.COND_EQUAL, c3)])
            rec = self.fetch(query)
            while rec is not None:
                c1val = int(wgdb.get_field(self.d, rec, 0))
                c1 = str(c1val + 99)
                wgdb.set_field(self.d, rec, 0, c1)
                rec = self.fetch(query)

        # Content check read queries, iteration 2
        for i in range(dbsize):
            val = str(10 * i + 99)
            self.check_matching_rows(0, wgdb.COND_EQUAL, val, 50*50)

        for i in range(50):
            val = 100 * i - 34555
            self.check_matching_rows(1, wgdb.COND_EQUAL, val, dbsize*50)

        for i in range(50):
            val = 1000 * i + 177889.576
            self.check_matching_rows(2, wgdb.COND_EQUAL, val, dbsize*50)

        # Delete query
        for i in range(dbsize):
            c1 = str(10 * i + 99)
            arglist = [ (0, wgdb.COND_EQUAL, c1),
                        (1, wgdb.COND_GREATER, -30556), # 10 matching
                        (2, wgdb.COND_LESSTHAN, 217889.575) # 40 matching
            ]
            query = wgdb.make_query(self.d, arglist = arglist)
            rec = self.fetch(query)
            while rec is not None:
                wgdb.delete_record(self.d, rec)
                rec = self.fetch(query)

        # Database scan
        self.check_db_rows(dbsize * (50 * 50 - 10 * 40))

class QueryParamTests(LowLevelQueryTest):
    """Test query parameter encoding through the wgdb module"""

    def test_params(self):
        """Test encoding parameters"""

        marker = "This is a marker"
        s1 = "ctGXioJeeUkTrxiSGaWxqFujCyWHJkmveMQXEnrHAMomjuPjKqUHlUtCVjOT"
        s2 = "zjXNNGYUBjmdCrLaAaKv"
        s3 = "GRvWOVYBMObOzWPqVFCt"
        s4 = "#eNijRGUJbuHoJEMxRUCQ"
        s5 = "http://example.com/?UQCOtBzWkdipHplZqwQF"
        s6 = "KqKVvVhVcxbLssirtydJ"
        s7 = "xsd:Name"

        # this row shouldn't be returned by the queries (except
        # the NULL query)
        rec0 = wgdb.create_record(self.d, 15)
        wgdb.set_field(self.d, rec0, 0, "This is not a marker")

        # this row should be returned by the queries
        rec = wgdb.create_record(self.d, 15)
        wgdb.set_field(self.d, rec, 0, marker)
        
        # CHARTYPE
        wgdb.set_field(self.d, rec, 1, "Z", wgdb.CHARTYPE)
        # DATETYPE
        wgdb.set_field(self.d, rec, 2, datetime.date(1943, 2, 28))
        # DOUBLETYPE
        wgdb.set_field(self.d, rec, 3, 105819.387451)
        # FIXPOINTTYPE
        wgdb.set_field(self.d, rec, 4, 783.799, wgdb.FIXPOINTTYPE)
        # INTTYPE
        wgdb.set_field(self.d, rec, 5, -871043)
        # NULLTYPE
        wgdb.set_field(self.d, rec, 7, None)
        # RECORDTYPE
        wgdb.set_field(self.d, rec, 8, rec0)
        # STRTYPE
        wgdb.set_field(self.d, rec, 9, s1)
        wgdb.set_field(self.d, rec, 10, s2, wgdb.STRTYPE)
        wgdb.set_field(self.d, rec, 11, s3, ext_str="en")
        # TIMETYPE
        wgdb.set_field(self.d, rec, 12, datetime.time(11, 22, 33))
        # URITYPE
        wgdb.set_field(self.d, rec, 13, s4, wgdb.URITYPE, s5)
        # XMLLITERALTYPE
        wgdb.set_field(self.d, rec, 14, s6, wgdb.XMLLITERALTYPE, s7)

        # CHARTYPE
        query = wgdb.make_query(self.d,
          arglist = [(1, wgdb.COND_EQUAL, ("Z", wgdb.CHARTYPE))])
        self.assertEqual(query.res_count, 1)
        rec = self.fetch(query)
        self.assertEqual(wgdb.get_field(self.d, rec, 0), marker)
        self.assertIsNone(self.fetch(query))

        # DATETYPE
        query = wgdb.make_query(self.d,
          arglist = [(2, wgdb.COND_EQUAL, datetime.date(1943, 2, 28))])
        self.assertEqual(query.res_count, 1)
        rec = self.fetch(query)
        self.assertEqual(wgdb.get_field(self.d, rec, 0), marker)
        self.assertIsNone(self.fetch(query))

        # DOUBLETYPE
        query = wgdb.make_query(self.d,
          arglist = [(3, wgdb.COND_EQUAL, 105819.387451)])
        self.assertEqual(query.res_count, 1)
        rec = self.fetch(query)
        self.assertEqual(wgdb.get_field(self.d, rec, 0), marker)
        self.assertIsNone(self.fetch(query))

        # FIXPOINTTYPE
        query = wgdb.make_query(self.d,
          arglist = [(4, wgdb.COND_EQUAL, (783.799, wgdb.FIXPOINTTYPE))])
        self.assertEqual(query.res_count, 1)
        rec = self.fetch(query)
        self.assertEqual(wgdb.get_field(self.d, rec, 0), marker)
        self.assertIsNone(self.fetch(query))

        # INTTYPE
        query = wgdb.make_query(self.d,
          arglist = [(5, wgdb.COND_EQUAL, -871043)])
        self.assertEqual(query.res_count, 1)
        rec = self.fetch(query)
        self.assertEqual(wgdb.get_field(self.d, rec, 0), marker)
        self.assertIsNone(self.fetch(query))

        # NULLTYPE
        query = wgdb.make_query(self.d,
          arglist = [(7, wgdb.COND_EQUAL, None)])
        self.assertEqual(query.res_count, 2)
        self.assertIsNotNone(self.fetch(query))
        self.assertIsNotNone(self.fetch(query))
        self.assertIsNone(self.fetch(query))

        # RECORDTYPE
        query = wgdb.make_query(self.d,
          arglist = [(8, wgdb.COND_EQUAL, rec0)])
        self.assertEqual(query.res_count, 1)
        rec = self.fetch(query)
        self.assertEqual(wgdb.get_field(self.d, rec, 0), marker)
        self.assertIsNone(self.fetch(query))

        # STRTYPE
        query = wgdb.make_query(self.d,
          arglist = [(9, wgdb.COND_EQUAL, s1)])
        self.assertEqual(query.res_count, 1)
        rec = self.fetch(query)
        self.assertEqual(wgdb.get_field(self.d, rec, 0), marker)
        self.assertIsNone(self.fetch(query))

        query = wgdb.make_query(self.d,
          arglist = [(10, wgdb.COND_EQUAL, (s2, wgdb.STRTYPE))])
        self.assertEqual(query.res_count, 1)
        rec = self.fetch(query)
        self.assertEqual(wgdb.get_field(self.d, rec, 0), marker)
        self.assertIsNone(self.fetch(query))

        query = wgdb.make_query(self.d,
          arglist = [(11, wgdb.COND_EQUAL, (s3, wgdb.STRTYPE, "en"))])
        self.assertEqual(query.res_count, 1)
        rec = self.fetch(query)
        self.assertEqual(wgdb.get_field(self.d, rec, 0), marker)
        self.assertIsNone(self.fetch(query))

        # TIMETYPE
        query = wgdb.make_query(self.d,
          arglist = [(12, wgdb.COND_EQUAL, datetime.time(11, 22, 33))])
        self.assertEqual(query.res_count, 1)
        rec = self.fetch(query)
        self.assertEqual(wgdb.get_field(self.d, rec, 0), marker)
        self.assertIsNone(self.fetch(query))

        # URITYPE
        query = wgdb.make_query(self.d,
          arglist = [(13, wgdb.COND_EQUAL, (s4, wgdb.URITYPE, s5))])
        self.assertEqual(query.res_count, 1)
        rec = self.fetch(query)
        self.assertEqual(wgdb.get_field(self.d, rec, 0), marker)
        self.assertIsNone(self.fetch(query))

        # XMLLITERALTYPE
        query = wgdb.make_query(self.d,
          arglist = [(14, wgdb.COND_EQUAL, (s6, wgdb.XMLLITERALTYPE, s7))])
        self.assertEqual(query.res_count, 1)
        rec = self.fetch(query)
        self.assertEqual(wgdb.get_field(self.d, rec, 0), marker)
        self.assertIsNone(self.fetch(query))

class WhiteDBTest(unittest.TestCase):
    """Provide setUp()/tearDown() for test cases that
    use the WhiteDB module API."""

    def setUp(self):
        self.d = whitedb.connect(shmsize=MINDBSIZE, local=1)

    def tearDown(self):
        self.d.close()

    def check_db_rows(self, expected):
        """Count db rows."""

        cnt = 0
        rec = self.d.first_record()
        while rec is not None:
            cnt += 1
            rec = self.d.next_record(rec)
        self.assertEqual(cnt, expected)

class WhiteDBConnection(WhiteDBTest):
    """Test WhiteDB connection class methods. Does not cover
        the functionality that is normally accessed through
        Cursor and Record classes"""

    def test_creation(self):
        """Tests record creation and low level
        scanning to retrieve records from the database."""

        rec = self.d.create_record(3)
        self.assertTrue(isinstance(rec, whitedb.Record))

        rec = self.d.atomic_create_record([0, 0, 0])
        self.assertTrue(isinstance(rec, whitedb.Record))

        rec = self.d.insert([0, 0, 0])
        self.assertTrue(isinstance(rec, whitedb.Record))

        with self.assertRaises(whitedb.DataError):
            self.d.insert([])

        with self.assertRaises(whitedb.DataError):
            self.d.create_record(-3)

    def test_fielddata(self):
        """Test field data reading and writing on connection
        level. This would be normally accessed through the record,
        but we depend on these functions to check the first/next records"""

        rec = self.d.create_record(20)
        self.d.set_field(rec, 6, 372296787) # regular data
        self.d.set_field(rec, 13, "2467305",
            whitedb.wgdb.CHARTYPE) # data with encoding
        self.d.set_field(rec, 19, "#907735743", whitedb.wgdb.URITYPE,
            "http://unittest/") # data with extstr

        self.assertEqual(self.d.get_field(rec, 6), 372296787)
        self.assertEqual(self.d.get_field(rec, 13), "2")
        self.assertEqual(self.d.get_field(rec, 19),
            "http://unittest/#907735743")

    def test_firstnext(self):
        """Test fetching the first and next record"""
        
        self.d.insert([112060684])
        self.d.insert([566973731])

        rec = self.d.first_record()
        self.assertEqual(self.d.get_field(rec, 0), 112060684)
        rec = self.d.next_record(rec)
        self.assertEqual(self.d.get_field(rec, 0), 566973731)

class WhiteDBRecord(WhiteDBTest):
    """Test WhiteDB Record class"""

    def test_highlevel(self):
        """Tests high level record functionality."""

        rec = self.d.insert([197622332,
            (2.67985826, whitedb.wgdb.DOUBLETYPE),
            ("874485001", whitedb.wgdb.XMLLITERALTYPE,"xsd:integer")
            ])
        self.assertTrue(isinstance(rec, whitedb.Record))

        self.assertEqual(len(rec), 3)
        self.assertEqual(rec[0], 197622332)
        self.assertAlmostEqual(rec[1], 2.67985826)
        self.assertEqual(rec[2], "874485001")

        # XXX: test len() here once implemented.

    def test_deletion(self):
        """Tests deleting a record"""
        
        self.check_db_rows(0)
        
        rec = self.d.insert([None])
        self.check_db_rows(1)
        
        rec.delete()
        self.check_db_rows(0)

    def test_update(self):
        """Test record updating"""

        rec = self.d.insert([None, None, None, None, 630781304])
        rec.update(["This", "is", "an", "update", 345849564])

        self.assertEqual(rec[0], "This")
        self.assertEqual(rec[1], "is")
        self.assertEqual(rec[2], "an")
        self.assertEqual(rec[3], "update")
        self.assertEqual(rec[4], 345849564)

        with self.assertRaises(whitedb.wgdb.error):
            # too long
            rec.update([None, None, None, None, 630781304, None])

        # nevertheless, fields that fit are overwritten
        self.assertEqual(rec[4], 630781304)

    def test_fielddata(self):
        """Test set and get field functions"""

        rec = self.d.create_record(3)
        rec.set_field(0, "168691904")
        
        with self.assertRaises(TypeError):
            rec.set_field(1, ("notanumber", whitedb.wgdb.INTTYPE))

        with self.assertRaises(TypeError):
            rec.set_field(2, (248557089, 959010401))

        with self.assertRaises(whitedb.DataError):
            rec.set_field(3, "no such field")

        self.assertEqual(rec.get_field(0), "168691904")
        self.assertEqual(rec.get_field(1), None)
        self.assertEqual(rec.get_field(2), None)

        with self.assertRaises(whitedb.DataError):
            rec.get_field(3)

    def test_getsize(self):
        """Test record size helper function"""

        rec = self.d.create_record(275)
        self.assertEqual(rec.get_size(), 275)

        l = [ 0, 0, 0, 0 ]
        rec = self.d.insert(l)
        self.assertEqual(rec.get_size(), len(l))

    def test_linkrec(self):
        """Test linked records"""

        rec = self.d.insert([737483554])
        rec2 = self.d.insert([859310257, rec])

        self.assertTrue(isinstance(rec2[1], whitedb.Record))
        self.assertEqual(rec2[1][0], 737483554)

        rec[0] = 284107294
        self.assertEqual(rec2.get_field(1).get_field(0), 284107294)

class WhiteDBCursor(WhiteDBTest):
    """Test WhiteDB Cursor class"""

    def make_testdata(self):
        rows = [
            [5038, 933, 2513, 3743, 1068],
            [1459, 6185, 8457, 277, 171],
            [7261, 9882, 172, 7034, 755],
            [3751, 3690, 9976, 1225, 5825],
            [9910, 8478, 595, 924, 8804],
            [6801, 745, 5993, 6331, 7807],
            [5255, 2481, 595, 5685, 8532],
            [4579, 9155, 595, 478, 1167],
            [6753, 3518, 5928, 9286, 1637],
            [2781, 3919, 786, 9286, 7953]
        ]
        for row in rows:
            self.d.insert(row)
    
    def count_results(self, cur):
        cnt = 0
        while cur.fetchone() is not None:
            cnt += 1
        return cnt

    def test_basic(self):
        """Tests record creation and low level
        scanning to retrieve records from the database."""

        cur = self.d.cursor()
        self.assertTrue(isinstance(cur, whitedb.Cursor))
        
        cur.execute()
        self.assertIsNone(cur.fetchone())

        self.d.insert([None, None, 846516765])
        cur.execute()
        self.assertEqual(cur.rowcount, 1)
        rec = cur.fetchone()
        self.assertEqual(rec[2], 846516765)

    def test_matchrec(self):
        """Test query with a match record"""

        self.make_testdata()
        wildcard = (0, whitedb.wgdb.VARTYPE)
        cur = self.d.cursor()

        # list matchrec
        cur.execute(matchrec = [wildcard, wildcard, wildcard, 9286, wildcard])
        self.assertEqual(cur.rowcount, 2)
        self.assertEqual(self.count_results(cur), 2)

        cur.execute(matchrec = [wildcard, wildcard, wildcard, 9286, 7953])
        self.assertEqual(cur.rowcount, 1)
        self.assertEqual(self.count_results(cur), 1)

        cur.execute(matchrec = [None, wildcard, wildcard, 9286, 7953])
        self.assertEqual(cur.rowcount, 0)
        self.assertEqual(self.count_results(cur), 0)

        # shorter record with matching field values
        cur.execute(matchrec = [5038, 933, 2513])
        self.assertEqual(cur.rowcount, 1)
        self.assertEqual(self.count_results(cur), 1)

        # actual record matchrec
        rec = self.d.insert([wildcard, wildcard, 595, wildcard, wildcard])
        cur.execute(matchrec = rec)
        self.assertEqual(cur.rowcount, 4)
        self.assertEqual(self.count_results(cur), 4)

        # shorter record with matching field values
        rec = self.d.insert([2781, 3919, 786, 9286])
        cur.execute(matchrec = rec)
        self.assertEqual(cur.rowcount, 2)
        self.assertEqual(self.count_results(cur), 2)
        
    def test_arglist(self):
        """Test query with an argument list"""

        self.make_testdata()
        cur = self.d.cursor()

        # one condition: COND_EQUAL
        cur.execute(arglist = [(2, wgdb.COND_EQUAL, 595)])
        self.assertEqual(cur.rowcount, 3)
        self.assertEqual(self.count_results(cur), 3)

        # inverse of previous query: COND_NOT_EQUAL
        cur.execute(arglist = [(2, wgdb.COND_NOT_EQUAL, 595)])
        self.assertEqual(cur.rowcount, 7)
        self.assertEqual(self.count_results(cur), 7)

        # two conditions: COND_LESSTHAN, COND_GREATER
        cur.execute(arglist = [(0, wgdb.COND_LESSTHAN, 6801),
            (4, wgdb.COND_GREATER, 1637)])
        self.assertEqual(cur.rowcount, 3)
        self.assertEqual(self.count_results(cur), 3)

        # inclusive versions of previous query: COND_LTEQUAL, COND_GTEQUAL
        cur.execute(arglist = [(0, wgdb.COND_LTEQUAL, 6801),
            (4, wgdb.COND_GTEQUAL, 1637)])
        self.assertEqual(cur.rowcount, 5)
        self.assertEqual(self.count_results(cur), 5)

    def test_fetch(self):
        """Test the fetchall() and fetchone() functions"""

        self.make_testdata()
        cur = self.d.cursor()

        with self.assertRaises(whitedb.ProgrammingError):
            cur.fetchone()

        cur.execute(arglist = [(3, wgdb.COND_NOT_EQUAL, 9286)])
        self.assertEqual(cur.rowcount, 8)
        rows = cur.fetchall()
        self.assertEqual(len(rows), 8)
        for row in rows:
            self.assertNotEqual(row[3], 9286)

        cur.execute(arglist = [(3, wgdb.COND_NOT_EQUAL, 9286)])
        self.assertEqual(cur.rowcount, 8)
        cnt = 0
        row = cur.fetchone()
        while row is not None:
            cnt += 1
            self.assertNotEqual(row[3], 9286)
            row = cur.fetchone()
        self.assertEqual(cnt, 8)

        cur.execute(arglist = [(3, wgdb.COND_NOT_EQUAL, 9286)])
        cur.close()
        with self.assertRaises(whitedb.ProgrammingError):
            cur.fetchone()

if __name__ == "__main__":
    unittest.main()
