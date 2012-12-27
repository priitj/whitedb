#!/usr/bin/env python
# -*- coding: latin-1 -*-
#
# Copyright (c) Priit Järv 2012
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

"""@file tests.py

Unit tests for the WGandalf Python API
"""

import unittest

import wgdb
import WGandalf

import datetime

MINDBSIZE=800000 # should cover 64-bit databases that need more memory

# Currently we use the expensive database creation for each database
# test. This wastes the CPU but is supposedly following the unit test
# philosophy of isolating each test case.
#
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
        # management of WGandalf. By the API description, the records
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

if __name__ == "__main__":
    unittest.main()
