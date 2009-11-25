/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2009
*
* This file is part of wgandalf
*
* Wgandalf is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
* 
* Wgandalf is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
* 
* You should have received a copy of the GNU General Public License
* along with Wgandalf.  If not, see <http://www.gnu.org/licenses/>.
*
*/

 /** @file wgdbmodule.c
 *  Python extension module for accessing wgandalf database
 *
 */

/* ====== Includes =============== */

#include <Python.h>
#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

#include "dbapi.h"

/* ====== Private headers and defs ======== */


/* ======= Private protos ================ */

static PyObject * wgdb_attach_database(PyObject *self, PyObject *args);
static PyObject * wgdb_delete_database(PyObject *self, PyObject *args);

/* ============= Private vars ============ */

/** Module exception object */
static PyObject *wgdb_error;

/** Method table */
static PyMethodDef wgdb_methods[] = {
  {"attach_database",  wgdb_attach_database, METH_VARARGS,
   "Connect to a shared memory database. If the database with the "\
   "given name does not exist, it is created."},
  {"delete_database",  wgdb_delete_database, METH_VARARGS,
   "Delete a shared memory database."},
  {NULL, NULL, 0, NULL} /* terminator */
};

/* ============== Functions ============== */

/** Attach to memory database.
 *  Python wrapper to wg_attach_database()
 */

static PyObject * wgdb_attach_database(PyObject *self, PyObject *args) {
  /* dummy */
  return Py_None;
}

/** Delete memory database.
 *  Python wrapper to wg_delete_database()
 */

static PyObject * wgdb_delete_database(PyObject *self, PyObject *args) {
  /* dummy */
  return Py_None;
}

/** Initialize module.
 *  Standard entry point for Python extension modules, executed
 *  during import.
 */

PyMODINIT_FUNC initwgdb(void) {
  PyObject *m;
  
  m = Py_InitModule3("wgdb", wgdb_methods, "wgandalf database adapter");
  if(!m) return;

  wgdb_error = PyErr_NewException("wgdb.error", NULL, NULL);
  Py_INCREF(wgdb_error);
  PyModule_AddObject(m, "error", wgdb_error);  
}
