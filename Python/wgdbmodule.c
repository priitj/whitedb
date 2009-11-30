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

#include "dbmem.h"
#include "dbapi.h"

/* ====== Private headers and defs ======== */

typedef struct {
  PyObject_HEAD
  void *db;
} wg_database;

/* ======= Private protos ================ */

static PyObject * wgdb_attach_database(PyObject *self, PyObject *args,
                                        PyObject *kwds);
static PyObject * wgdb_delete_database(PyObject *self, PyObject *args);

static PyObject *wg_database_repr(wg_database *obj);

/* ============= Private vars ============ */

/** Module exception object */
static PyObject *wgdb_error;

/** Database object type */
static PyTypeObject wg_database_type = {
  PyObject_HEAD_INIT(NULL)
  0,                            /*ob_size*/
  "wgdb.Database",              /*tp_name*/
  sizeof(wg_database),          /*tp_basicsize*/
  0,                            /*tp_itemsize*/
  0,                            /*tp_dealloc*/
  0,                            /*tp_print*/
  0,                            /*tp_getattr*/
  0,                            /*tp_setattr*/
  0,                            /*tp_compare*/
  (reprfunc) wg_database_repr,  /*tp_repr*/
  0,                            /*tp_as_number*/
  0,                            /*tp_as_sequence*/
  0,                            /*tp_as_mapping*/
  0,                            /*tp_hash */
  0,                            /*tp_call*/
  (reprfunc) wg_database_repr,  /*tp_str*/
  0,                            /*tp_getattro*/
  0,                            /*tp_setattro*/
  0,                            /*tp_as_buffer*/
  Py_TPFLAGS_DEFAULT,           /*tp_flags*/
  "WGandalf database object",   /* tp_doc */
};

/** Method table */
static PyMethodDef wgdb_methods[] = {
  {"attach_database",  (PyCFunction) wgdb_attach_database,
   METH_VARARGS | METH_KEYWORDS,
   "Connect to a shared memory database. If the database with the "\
   "given name does not exist, it is created."},
  {"delete_database",  wgdb_delete_database, METH_VARARGS,
   "Delete a shared memory database."},
  {NULL, NULL, 0, NULL} /* terminator */
};

/* ============== Functions ============== */

/* Wrapped wgdb API
 * uses wg_database_type object to store the database pointer
 * when making calls from python. This type is not available
 * generally (using non-restricted values for the pointer
 * would cause segfaults), only by calling wgdb_attach_database().
 */

/** Attach to memory database.
 *  Python wrapper to wg_attach_database()
 */

static PyObject * wgdb_attach_database(PyObject *self, PyObject *args,
                                        PyObject *kwds) {
  wg_database *db;
  char *shmname = NULL;
  wg_int sz = 0;
  static char *kwlist[] = {"shmname", "size", NULL};

  if(!PyArg_ParseTupleAndKeywords(args, kwds, "|si", kwlist, &shmname, &sz))
    return NULL;
  
  db = (wg_database *) wg_database_type.tp_alloc(&wg_database_type, 0);
  if(!db) return NULL;

  /* Now try to actually connect. Note that this may create
   * a new database if none is found with a matching name.
   */
/*  db->db = (void *) 0; */
  db->db = (void *) wg_attach_database(shmname, sz);
  if(!db->db) {
    PyErr_SetString(wgdb_error, "Failed to attach to database.");
    /* XXX: should we free it here or does the garbage collector
     * pick it up anyway? (no references)
     */
    wg_database_type.tp_free(db);
    return NULL;
  }
  Py_INCREF(db);
  return (PyObject *) db;
}

/** Delete memory database.
 *  Python wrapper to wg_delete_database()
 */

static PyObject * wgdb_delete_database(PyObject *self, PyObject *args) {
  char *shmname = NULL;
  int err = 0;

  if(!PyArg_ParseTuple(args, "|s", &shmname))
    return NULL;

  err = wg_delete_database(shmname);
  if(err) {
    PyErr_SetString(wgdb_error, "Failed to delete the database.");
    return NULL;
  }

  Py_INCREF(Py_None);
  return Py_None;
}

/* checking if argument is of correct type (for functions that
  take db pointer as argument)
  if(!PyObject_TypeCheck(db, &wg_database_type)) {
    PyErr_SetString(PyExc_TypeError,
      "Argument must be a WGandalf database object.");
    return NULL;
  }
*/

/* Methods for the wg_database_type objects
 * XXX: might need _dealloc() that calls wg_detach_database()
 */

/** String representation of database object. This is used for both
 * repr() and str()
 */
static PyObject *wg_database_repr(wg_database * obj) {
  /* XXX: this is incompatible with eval(). If a need to
   * eval() such representations should arise, new initialization
   * function is also needed for the type.
   */
  return PyString_FromFormat("<WGandalf database at %x>",
    (unsigned int) obj->db);
}

/** Initialize module.
 *  Standard entry point for Python extension modules, executed
 *  during import.
 */

PyMODINIT_FUNC initwgdb(void) {
  PyObject *m;
  
  wg_database_type.tp_new = PyType_GenericNew;
  if (PyType_Ready(&wg_database_type) < 0)
    return;
  
  m = Py_InitModule3("wgdb", wgdb_methods, "wgandalf database adapter");
  if(!m) return;

  wgdb_error = PyErr_NewException("wgdb.error", NULL, NULL);
  Py_INCREF(wgdb_error);
  PyModule_AddObject(m, "error", wgdb_error);  
}
