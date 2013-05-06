#!/usr/bin/env python
# -*- coding: latin-1 -*-
#
# Copyright (c) Priit Järv 2013
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

"""@file WGandalf.py

Backwards compatibility wrapper for WhiteDB database Python API
"""

from warnings import warn
warn("WGandalf module is deprecated, use whitedb instead", DeprecationWarning)

from whitedb import *
