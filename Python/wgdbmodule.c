/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2009, 2010, 2013
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

 /** @file wgdbmodule.c
 *  Python extension module for accessing WhiteDB database
 *
 */

/* ====== Includes =============== */

#include <Python.h>
#include <datetime.h>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

#include "dbapi.h"

/* ====== Private headers and defs ======== */

#if PY_VERSION_HEX >= 0x03000000
#define PYTHON3
#define ENCODEERR "surrogateescape" /* handling of charset mismatches */
#if PY_VERSION_HEX >= 0x03030000
#define HAVE_LOCALEENC /* locale dependent string encode function exists */
#endif
#endif

struct module_state {
  PyObject *wgdb_error;
};

typedef struct {
  PyObject_HEAD
  void *db;
  int local;
} wg_database;

typedef struct {
  PyObject_HEAD
  void *rec;
} wg_record;

typedef struct {
  PyObject_HEAD
  wg_query *query;
  wg_database *db;
  wg_query_arg *arglist;
  int argc;
  void *matchrec;
  int reclen;
} wg_query_ob;  /* append _ob to avoid name clash with dbapi.h */


/* ======= Private protos ================ */

static PyObject *wgdb_attach_database(PyObject *self, PyObject *args,
                                        PyObject *kwds);
static PyObject *wgdb_attach_existing_database(PyObject *self, PyObject *args);
static PyObject *wgdb_delete_database(PyObject *self, PyObject *args);
static PyObject *wgdb_detach_database(PyObject *self, PyObject *args);

static PyObject *wgdb_create_record(PyObject *self, PyObject *args);
static PyObject *wgdb_create_raw_record(PyObject *self, PyObject *args);
static PyObject *wgdb_get_first_record(PyObject *self, PyObject *args);
static PyObject *wgdb_get_next_record(PyObject *self, PyObject *args);
static PyObject *wgdb_get_record_len(PyObject *self, PyObject *args);
static PyObject *wgdb_is_record(PyObject *self, PyObject *args);
static PyObject *wgdb_delete_record(PyObject *self, PyObject *args);

static wg_int pytype_to_wgtype(PyObject *data, wg_int ftype);
static wg_int encode_pyobject_null(wg_database *db);
static wg_int encode_pyobject_record(wg_database *db, PyObject *data);
static wg_int encode_pyobject_int(wg_database *db, PyObject *data, int param);
static wg_int encode_pyobject_double(wg_database *db, PyObject *data,
  int param);
static wg_int encode_pyobject_str(wg_database *db, PyObject *data,
  char *ext_str, int param);
static wg_int encode_pyobject_uri(wg_database *db, PyObject *data,
  char *ext_str, int param);
static wg_int encode_pyobject_xmlliteral(wg_database *db, PyObject *data,
  char *ext_str, int param);
static wg_int encode_pyobject_char(wg_database *db, PyObject *data,
  int param);
static wg_int encode_pyobject_fixpoint(wg_database *db, PyObject *data,
  int param);
static wg_int encode_pyobject_date(wg_database *db, PyObject *data,
  int param);
static wg_int encode_pyobject_time(wg_database *db, PyObject *data,
  int param);
static wg_int encode_pyobject_var(wg_database *db, PyObject *data,
  int param);
static wg_int encode_pyobject(wg_database *db, PyObject *data,
                            wg_int ftype, char *ext_str, int param);
static wg_int encode_pyobject_ext(PyObject *self,
                            wg_database *db, PyObject *obj, int param);
static PyObject *wgdb_set_field(PyObject *self, PyObject *args,
                                        PyObject *kwds);
static PyObject *wgdb_set_new_field(PyObject *self, PyObject *args,
                                        PyObject *kwds);
static PyObject *wgdb_get_field(PyObject *self, PyObject *args);

static PyObject *wgdb_start_write(PyObject *self, PyObject *args);
static PyObject *wgdb_end_write(PyObject *self, PyObject *args);
static PyObject *wgdb_start_read(PyObject *self, PyObject *args);
static PyObject *wgdb_end_read(PyObject *self, PyObject *args);

static int parse_query_params(PyObject *self, PyObject *args,
                                    PyObject *kwds, wg_query_ob *query);
static PyObject * wgdb_make_query(PyObject *self, PyObject *args,
                                        PyObject *kwds);
static PyObject * wgdb_fetch(PyObject *self, PyObject *args);
static PyObject * wgdb_free_query(PyObject *self, PyObject *args);
static void free_query(wg_query_ob *obj);

static void wg_database_dealloc(wg_database *obj);
static void wg_query_dealloc(wg_query_ob *obj);
static PyObject *wg_database_repr(wg_database *obj);
static PyObject *wg_record_repr(wg_record *obj);
static PyObject *wg_query_repr(wg_query_ob *obj);
static PyObject *wg_query_get_res_count(wg_query_ob *obj, void *closure);
static int wg_query_set_res_count(wg_query_ob *obj,
                                      PyObject *value, void *closure);
static void wgdb_error_setstring(PyObject *self, char *err);

/* ============= Private vars ============ */

/** Module state, contains the exception object */
#ifndef PYTHON3
static struct module_state _state;
#endif

/** Database object type */
static PyTypeObject wg_database_type = {
#ifndef PYTHON3
  PyObject_HEAD_INIT(NULL)
  0,                            /*ob_size*/
#else
  PyVarObject_HEAD_INIT(NULL, 0)
#endif
  "wgdb.Database",              /*tp_name*/
  sizeof(wg_database),          /*tp_basicsize*/
  0,                            /*tp_itemsize*/
  (destructor) wg_database_dealloc, /*tp_dealloc*/
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
  "WhiteDB database object",    /* tp_doc */
};

/** Record object type */
static PyTypeObject wg_record_type = {
#ifndef PYTHON3
  PyObject_HEAD_INIT(NULL)
  0,                            /*ob_size*/
#else
  PyVarObject_HEAD_INIT(NULL, 0)
#endif
  "wgdb.Record",                /*tp_name*/
  sizeof(wg_record),            /*tp_basicsize*/
  0,                            /*tp_itemsize*/
  0,                            /*tp_dealloc*/
  0,                            /*tp_print*/
  0,                            /*tp_getattr*/
  0,                            /*tp_setattr*/
  0,                            /*tp_compare*/
  (reprfunc) wg_record_repr,    /*tp_repr*/
  0,                            /*tp_as_number*/
  0,                            /*tp_as_sequence*/
  0,                            /*tp_as_mapping*/
  0,                            /*tp_hash */
  0,                            /*tp_call*/
  (reprfunc) wg_record_repr,    /*tp_str*/
  0,                            /*tp_getattro*/
  0,                            /*tp_setattro*/
  0,                            /*tp_as_buffer*/
  Py_TPFLAGS_DEFAULT,           /*tp_flags*/
  "WhiteDB record object",      /* tp_doc */
};

/** Data accessor functions for the Query type */
static PyGetSetDef wg_query_getset[] = {
  {"res_count",                         /* attribyte name */
   (getter) wg_query_get_res_count,
   (setter) wg_query_set_res_count,
   "Number of rows in the result set",  /* doc */
   NULL},                               /* closure, not used here */
  {NULL}
};

/** Query object type */
static PyTypeObject wg_query_type = {
#ifndef PYTHON3
  PyObject_HEAD_INIT(NULL)
  0,                            /*ob_size*/
#else
  PyVarObject_HEAD_INIT(NULL, 0)
#endif
  "wgdb.Query",                 /*tp_name*/
  sizeof(wg_query_ob),          /*tp_basicsize*/
  0,                            /*tp_itemsize*/
  (destructor) wg_query_dealloc, /*tp_dealloc*/
  0,                            /*tp_print*/
  0,                            /*tp_getattr*/
  0,                            /*tp_setattr*/
  0,                            /*tp_compare*/
  (reprfunc) wg_query_repr,     /*tp_repr*/
  0,                            /*tp_as_number*/
  0,                            /*tp_as_sequence*/
  0,                            /*tp_as_mapping*/
  0,                            /*tp_hash */
  0,                            /*tp_call*/
  (reprfunc) wg_query_repr,     /*tp_str*/
  0,                            /*tp_getattro*/
  0,                            /*tp_setattro*/
  0,                            /*tp_as_buffer*/
  Py_TPFLAGS_DEFAULT,           /*tp_flags*/
  "WhiteDB query object",       /* tp_doc */
  0,                            /* tp_traverse */
  0,                            /* tp_clear */
  0,                            /* tp_richcompare */
  0,                            /* tp_weaklistoffset */
  0,                            /* tp_iter */
  0,                            /* tp_iternext */
  0,                            /* tp_methods */
  0,                            /* tp_members */
  wg_query_getset,              /* tp_getset */
};

/** Method table */
static PyMethodDef wgdb_methods[] = {
  {"attach_database",  (PyCFunction) wgdb_attach_database,
   METH_VARARGS | METH_KEYWORDS,
   "Connect to a shared memory database. If the database with the "\
   "given name does not exist, it is created."},
  {"attach_existing_database",  (PyCFunction) wgdb_attach_existing_database,
   METH_VARARGS,
   "Connect to a shared memory database. Fails if the database with the "\
   "given name does not exist."},
  {"delete_database",  wgdb_delete_database, METH_VARARGS,
   "Delete a shared memory database."},
  {"detach_database",  wgdb_detach_database, METH_VARARGS,
   "Detach from shared memory database."},
  {"create_record",  wgdb_create_record, METH_VARARGS,
   "Create a record with given length."},
  {"create_raw_record",  wgdb_create_raw_record, METH_VARARGS,
   "Create a record without indexing the fields."},
  {"get_first_record",  wgdb_get_first_record, METH_VARARGS,
   "Fetch first record from database."},
  {"get_next_record",  wgdb_get_next_record, METH_VARARGS,
   "Fetch next record from database."},
  {"get_record_len",  wgdb_get_record_len, METH_VARARGS,
   "Get record length (number of fields)."},
  {"is_record",  wgdb_is_record, METH_VARARGS,
   "Determine if object is a WhiteDB record."},
  {"delete_record",  wgdb_delete_record, METH_VARARGS,
   "Delete a record."},
  {"set_field",  (PyCFunction) wgdb_set_field,
   METH_VARARGS | METH_KEYWORDS,
   "Set field value."},
  {"set_new_field",  (PyCFunction) wgdb_set_new_field,
   METH_VARARGS | METH_KEYWORDS,
   "Set field value (assumes no previous content)."},
  {"get_field",  wgdb_get_field, METH_VARARGS,
   "Get field data decoded to corresponding Python type."},
  {"start_write",  wgdb_start_write, METH_VARARGS,
   "Start writing transaction."},
  {"end_write",  wgdb_end_write, METH_VARARGS,
   "Finish writing transaction."},
  {"start_read",  wgdb_start_read, METH_VARARGS,
   "Start reading transaction."},
  {"end_read",  wgdb_end_read, METH_VARARGS,
   "Finish reading transaction."},
  {"make_query",  (PyCFunction) wgdb_make_query,
   METH_VARARGS | METH_KEYWORDS,
   "Create a query object."},
  {"fetch",  wgdb_fetch, METH_VARARGS,
   "Fetch next record from a query."},
  {"free_query",  wgdb_free_query, METH_VARARGS,
   "Unallocates the memory (local and shared) used by the query."},
  {NULL, NULL, 0, NULL} /* terminator */
};

#ifdef PYTHON3
static struct PyModuleDef wgdb_def = {
   PyModuleDef_HEAD_INIT,
   "wgdb",                          /* name of module */
   "WhiteDB database adapter",      /* module documentation, may be NULL */
   sizeof(struct module_state),     /* size of per-interpreter state */
   wgdb_methods
};
#endif

/* ============== Functions ============== */

/* Wrapped wgdb API
 * uses wg_database_type object to store the database pointer
 * when making calls from python. This type is not available
 * generally (using non-restricted values for the pointer
 * would cause segfaults), only by calling wgdb_attach_database().
 */

/* Functions for attaching and deleting */

/** Attach to memory database.
 *  Python wrapper to wg_attach_database() and wg_attach_local_database()
 */

static PyObject * wgdb_attach_database(PyObject *self, PyObject *args,
                                        PyObject *kwds) {
  wg_database *db;
  char *shmname = NULL;
  wg_int sz = 0;
  wg_int local = 0;
  static char *kwlist[] = {"shmname", "size", "local", NULL};

  if(!PyArg_ParseTupleAndKeywords(args, kwds, "|snn",
    kwlist, &shmname, &sz, &local))
    return NULL;

  db = (wg_database *) wg_database_type.tp_alloc(&wg_database_type, 0);
  if(!db) return NULL;

  /* Now try to actually connect. Note that this may create
   * a new database if none is found with a matching name. In case of
   * a local database, a new one is allocated every time.
   */
  if(!local)
    db->db = (void *) wg_attach_database(shmname, sz);
  else
    db->db = (void *) wg_attach_local_database(sz);
  if(!db->db) {
    wgdb_error_setstring(self, "Failed to attach to database.");
    wg_database_type.tp_free(db);
    return NULL;
  }
  db->local = local;
/*  Py_INCREF(db);*/ /* XXX: not needed? if we increment here, the
                        object is never freed, even if it's unused */
  return (PyObject *) db;
}

/** Attach to memory database.
 *  Python wrapper to wg_attach_existing_database()
 */

static PyObject *wgdb_attach_existing_database(PyObject *self,
    PyObject *args) {
  wg_database *db;
  char *shmname = NULL;

  if(!PyArg_ParseTuple(args, "|s", &shmname))
    return NULL;

  db = (wg_database *) wg_database_type.tp_alloc(&wg_database_type, 0);
  if(!db) return NULL;

  /* Try to attach to an existing database. Fails if a database
   * with a matching name is not found. Only applies to shared
   * memory databases.
   */
  db->db = (void *) wg_attach_existing_database(shmname);
  if(!db->db) {
    wgdb_error_setstring(self, "Failed to attach to database.");
    wg_database_type.tp_free(db);
    return NULL;
  }
  db->local = 0;
/*  Py_INCREF(db);*/ /* XXX: not needed? if we increment here, the
                        object is never freed, even if it's unused */
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
    wgdb_error_setstring(self, "Failed to delete the database.");
    return NULL;
  }

  Py_INCREF(Py_None);
  return Py_None;
}

/** Detach from memory database.
 *  Python wrapper to wg_detach_database()
 *  Detaching is generally SysV-specific (so under Win32 this
 *  is currently a no-op).
 *  In case of a local database, wg_delete_local_database() is
 *  called instead.
 */

static PyObject * wgdb_detach_database(PyObject *self, PyObject *args) {
  PyObject *db = NULL;

  if(!PyArg_ParseTuple(args, "O!", &wg_database_type, &db))
    return NULL;

  /* Only try detaching if we have a valid pointer. */
  if(((wg_database *) db)->db) {
    if(((wg_database *) db)->local) {
      /* Local database should be deleted instead */
      wg_delete_local_database(((wg_database *) db)->db);
    } else if(wg_detach_database(((wg_database *) db)->db) < 0) {
      wgdb_error_setstring(self, "Failed to detach from database.");
      return NULL;
    }
    ((wg_database *) db)->db = NULL; /* mark as detached */
  }

  Py_INCREF(Py_None);
  return Py_None;
}

/* Functions to manipulate records. The record is also
 * represented as a custom type to avoid dealing with word
 * size issues on different platforms. So the type is essentially
 * a container for the record pointer.
 */

/** Create a record with given length.
 *  Python wrapper to wg_create_record()
 */

static PyObject * wgdb_create_record(PyObject *self, PyObject *args) {
  PyObject *db = NULL;
  wg_int length = 0;
  wg_record *rec;

  if(!PyArg_ParseTuple(args, "O!n", &wg_database_type, &db, &length))
    return NULL;

  /* Build a new record object */
  rec = (wg_record *) wg_record_type.tp_alloc(&wg_record_type, 0);
  if(!rec) return NULL;

  rec->rec = wg_create_record(((wg_database *) db)->db, length);
  if(!rec->rec) {
    wgdb_error_setstring(self, "Failed to create a record.");
    wg_record_type.tp_free(rec);
    return NULL;
  }

/*  Py_INCREF(rec);*/ /* XXX: not needed? */
  return (PyObject *) rec;
}

/** Create a record without indexing the fields.
 *  Python wrapper to wg_create_raw_record()
 */

static PyObject * wgdb_create_raw_record(PyObject *self, PyObject *args) {
  PyObject *db = NULL;
  wg_int length = 0;
  wg_record *rec;

  if(!PyArg_ParseTuple(args, "O!n", &wg_database_type, &db, &length))
    return NULL;

  /* Build a new record object */
  rec = (wg_record *) wg_record_type.tp_alloc(&wg_record_type, 0);
  if(!rec) return NULL;

  rec->rec = wg_create_raw_record(((wg_database *) db)->db, length);
  if(!rec->rec) {
    wgdb_error_setstring(self, "Failed to create a record.");
    wg_record_type.tp_free(rec);
    return NULL;
  }
  return (PyObject *) rec;
}

/** Fetch first record from database.
 *  Python wrapper to wg_get_first_record()
 */

static PyObject * wgdb_get_first_record(PyObject *self, PyObject *args) {
  PyObject *db = NULL;
  wg_record *rec;

  if(!PyArg_ParseTuple(args, "O!", &wg_database_type, &db))
    return NULL;

  /* Build a new record object */
  rec = (wg_record *) wg_record_type.tp_alloc(&wg_record_type, 0);
  if(!rec) return NULL;

  rec->rec = wg_get_first_record(((wg_database *) db)->db);
  if(!rec->rec) {
    wgdb_error_setstring(self, "Failed to fetch a record.");
    wg_record_type.tp_free(rec);
    return NULL;
  }

  Py_INCREF(rec);
  return (PyObject *) rec;
}

/** Fetch next record from database.
 *  Python wrapper to wg_get_next_record()
 */

static PyObject * wgdb_get_next_record(PyObject *self, PyObject *args) {
  PyObject *db = NULL, *prev = NULL;
  wg_record *rec;

  if(!PyArg_ParseTuple(args, "O!O!", &wg_database_type, &db,
      &wg_record_type, &prev))
    return NULL;

  /* Build a new record object */
  rec = (wg_record *) wg_record_type.tp_alloc(&wg_record_type, 0);
  if(!rec) return NULL;

  rec->rec = wg_get_next_record(((wg_database *) db)->db,
    ((wg_record *) prev)->rec);
  if(!rec->rec) {
    wgdb_error_setstring(self, "Failed to fetch a record.");
    wg_record_type.tp_free(rec);
    return NULL;
  }

  Py_INCREF(rec);
  return (PyObject *) rec;
}

/** Get record length (number of fields).
 *  Python wrapper to wg_get_record_len()
 */

static PyObject * wgdb_get_record_len(PyObject *self, PyObject *args) {
  PyObject *db = NULL, *rec = NULL;
  wg_int len = 0;

  if(!PyArg_ParseTuple(args, "O!O!", &wg_database_type, &db,
      &wg_record_type, &rec))
    return NULL;

  len = wg_get_record_len(((wg_database *) db)->db,
    ((wg_record *) rec)->rec);
  if(len < 0) {
    wgdb_error_setstring(self, "Failed to get the record length.");
    return NULL;
  }
  return Py_BuildValue("i", (int) len);
}

/** Determine, if object is a record
 *  Instead of exposing the record type directly as wgdb.Record,
 *  we provide this function. The reason is that we do not want
 *  these objects to be instantiated from Python, as such instances
 *  would have no valid record pointer to the memory database.
 */

static PyObject * wgdb_is_record(PyObject *self, PyObject *args) {
  PyObject *ob = NULL;

  if(!PyArg_ParseTuple(args, "O", &ob))
    return NULL;

  if(PyObject_TypeCheck(ob, &wg_record_type))
    return Py_BuildValue("i", 1);
  else
    return Py_BuildValue("i", 0);
}

/** Delete record.
 *  Python wrapper to wg_delete_record()
 */

static PyObject * wgdb_delete_record(PyObject *self, PyObject *args) {
  PyObject *db = NULL, *rec = NULL;
  wg_int err = 0;

  if(!PyArg_ParseTuple(args, "O!O!", &wg_database_type, &db,
      &wg_record_type, &rec))
    return NULL;

  err = wg_delete_record(((wg_database *) db)->db,
    ((wg_record *) rec)->rec);
  if(err == -1) {
    wgdb_error_setstring(self, "Record has references.");
    return NULL;
  }
  else if(err < -1) {
    wgdb_error_setstring(self, "Failed to delete record.");
    return NULL;
  }

  Py_INCREF(Py_None);
  return Py_None;
}


/* Functions to manipulate field contents.
 *
 * Storing data: the Python object is first converted to an appropriate
 * C data. Then wg_encode_*() is used to convert it to WhiteDB encoded
 * field data (possibly storing the actual data in the database, if the
 * object itself is hashed or does not fit in a field). The encoded data
 * is then stored with wg_set_field() or wg_set_new_field() as appropriate.
 *
 * Reading data: encoded field data is read using wg_get_field() and
 * examined to determine the type. If the type is recognized, the data
 * is converted to appropriate C data using wg_decode_*() family of
 * functions and finally to a Python object.
 */

/** Determine matching wgdb type of a Python object.
 *  ftype argument is a type hint in some cases where there's
 *  ambiguity due to multiple matching wgdb types.
 *
 *  returns -1 if the type is known, but the type hint is invalid.
 *  returns -2 if type is not recognized
 */
static wg_int pytype_to_wgtype(PyObject *data, wg_int ftype) {

  if(data==Py_None) {
    if(!ftype)
      return WG_NULLTYPE;
    else if(ftype!=WG_NULLTYPE)
      return -1;
  }
#ifndef PYTHON3
  else if(PyInt_Check(data)) {
#else
  else if(PyLong_Check(data)) {
#endif
    if(!ftype)
      return WG_INTTYPE;
    else if(ftype!=WG_INTTYPE && ftype!=WG_VARTYPE)
      return -1;
  }
  else if(PyFloat_Check(data)) {
    if(!ftype)
      return WG_DOUBLETYPE;
    else if(ftype!=WG_DOUBLETYPE && ftype!=WG_FIXPOINTTYPE)
      return -1;
  }
#ifndef PYTHON3
  else if(PyString_Check(data)) {
#else
  else if(PyUnicode_Check(data)) {
#endif
    if(!ftype)
      return WG_STRTYPE;
    else if(ftype!=WG_STRTYPE && ftype!=WG_CHARTYPE &&\
      ftype!=WG_URITYPE && ftype!=WG_XMLLITERALTYPE)
      return -1;
  }
  else if(PyObject_TypeCheck(data, &wg_record_type)) {
    if(!ftype)
      return WG_RECORDTYPE;
    else if(ftype!=WG_RECORDTYPE)
      return -1;
  }
  else if(PyDate_Check(data)) {
    if(!ftype)
      return WG_DATETYPE;
    else if(ftype!=WG_DATETYPE)
      return -1;
  }
  else if(PyTime_Check(data)) {
    if(!ftype)
      return WG_TIMETYPE;
    else if(ftype!=WG_TIMETYPE)
      return -1;
  }
  else
    /* Nothing matched */
    return -2;

  /* User-selected type was suitable */
  return ftype;
}

/** Encode an atomic value of type WG_NULLTYPE
 *  Always succeeds.
 */
static wg_int encode_pyobject_null(wg_database *db) {
  return wg_encode_null(db->db, 0);
}

/** Encode an atomic value of type WG_RECORDTYPE
 *  returns WG_ILLEGAL on failure
 */
static wg_int encode_pyobject_record(wg_database *db, PyObject *data) {
  return wg_encode_record(db->db, ((wg_record *) data)->rec);
}

/** Encode an atomic value of type WG_INTTYPE
 *  returns WG_ILLEGAL on failure
 *  if param is 1, the storage will be allocated in local memory (intended
 *  for encoding query parameters without write locking)
 */
static wg_int encode_pyobject_int(wg_database *db, PyObject *data, int param) {
  wg_int intdata;
#ifndef PYTHON3
  intdata = (wg_int) PyInt_AsLong(data);
#else
  intdata = (wg_int) PyLong_AsLong(data);
#endif

  if(!param) {
    return wg_encode_int(db->db, intdata);
  } else {
    return wg_encode_query_param_int(db->db, intdata);
  }
}

/** Encode an atomic value of type WG_DOUBLETYPE
 *  returns WG_ILLEGAL on failure
 *  if param is 1, the storage will be allocated in local memory (intended
 *  for encoding query parameters without write locking)
 */
static wg_int encode_pyobject_double(wg_database *db, PyObject *data,
  int param) {
  if(!param) {
    return wg_encode_double(db->db, (double) PyFloat_AsDouble(data));
  } else {
    return wg_encode_query_param_double(db->db,
      (double) PyFloat_AsDouble(data));
  }
}

/** Encode an atomic value of type WG_STRTYPE
 *  returns WG_ILLEGAL on failure
 *  if param is 1, the storage will be allocated in local memory (intended
 *  for encoding query parameters without write locking)
 */
static wg_int encode_pyobject_str(wg_database *db, PyObject *data,
  char *ext_str, int param) {
  char *s;
#ifndef PYTHON3
  s = PyString_AsString(data);
#elif defined(HAVE_LOCALEENC)
  s = PyBytes_AsString(PyUnicode_EncodeLocale(data, ENCODEERR));
#else
  s = PyBytes_AsString(PyUnicode_AsEncodedString(data, NULL, ENCODEERR));
#endif
  /* wg_encode_str is not guaranteed to check for NULL pointer */
  if(s) {
    if(!param) {
      return wg_encode_str(db->db, s, ext_str);
    } else {
      return wg_encode_query_param_str(db->db, s, ext_str);
    }
  } else {
    return WG_ILLEGAL;
  }
}

/** Encode an atomic value of type WG_URITYPE
 *  returns WG_ILLEGAL on failure
 *  if param is 1, the storage will be allocated in local memory (intended
 *  for encoding query parameters without write locking)
 */
static wg_int encode_pyobject_uri(wg_database *db, PyObject *data,
  char *ext_str, int param) {
  char *s;
#ifndef PYTHON3
  s = PyString_AsString(data);
#elif defined(HAVE_LOCALEENC)
  s = PyBytes_AsString(PyUnicode_EncodeLocale(data, ENCODEERR));
#else
  s = PyBytes_AsString(PyUnicode_AsEncodedString(data, NULL, ENCODEERR));
#endif
  /* wg_encode_str is not guaranteed to check for NULL pointer */
  if(s) {
    if(!param) {
      return wg_encode_uri(db->db, s, ext_str);
    } else {
      return wg_encode_query_param_uri(db->db, s, ext_str);
    }
  } else {
    return WG_ILLEGAL;
  }
}

/** Encode an atomic value of type WG_XMLLITERALTYPE
 *  returns WG_ILLEGAL on failure
 *  if param is 1, the storage will be allocated in local memory (intended
 *  for encoding query parameters without write locking)
 */
static wg_int encode_pyobject_xmlliteral(wg_database *db, PyObject *data,
  char *ext_str, int param) {
  char *s;
#ifndef PYTHON3
  s = PyString_AsString(data);
#elif defined(HAVE_LOCALEENC)
  s = PyBytes_AsString(PyUnicode_EncodeLocale(data, ENCODEERR));
#else
  s = PyBytes_AsString(PyUnicode_AsEncodedString(data, NULL, ENCODEERR));
#endif
  /* wg_encode_str is not guaranteed to check for NULL pointer */
  if(s) {
    if(!param) {
      return wg_encode_xmlliteral(db->db, s, ext_str);
    } else {
      return wg_encode_query_param_xmlliteral(db->db, s, ext_str);
    }
  } else {
    return WG_ILLEGAL;
  }
}

/** Encode an atomic value of type WG_CHARTYPE
 *  returns WG_ILLEGAL on failure
 *  if param is 1, value is encoded as a query parameter.
 */
static wg_int encode_pyobject_char(wg_database *db, PyObject *data,
  int param) {
  char *s;
#ifndef PYTHON3
  s = PyString_AsString(data);
#elif defined(HAVE_LOCALEENC)
  s = PyBytes_AsString(PyUnicode_EncodeLocale(data, ENCODEERR));
#else
  s = PyBytes_AsString(PyUnicode_AsEncodedString(data, NULL, ENCODEERR));
#endif
  /* wg_encode_str is not guaranteed to check for NULL pointer */
  if(s) {
    if(!param) {
      return wg_encode_char(db->db, s[0]);
    } else {
      return wg_encode_query_param_char(db->db, s[0]);
    }
  } else {
    return WG_ILLEGAL;
  }
}

/** Encode an atomic value of type WG_FIXPOINTTYPE
 *  returns WG_ILLEGAL on failure
 *  if param is 1, value is encoded as a query parameter.
 */
static wg_int encode_pyobject_fixpoint(wg_database *db, PyObject *data,
  int param) {
  if(!param) {
    return wg_encode_fixpoint(db->db, (double) PyFloat_AsDouble(data));
  } else {
    return wg_encode_query_param_fixpoint(db->db,
      (double) PyFloat_AsDouble(data));
  }
}

/** Encode an atomic value of type WG_DATETYPE
 *  returns WG_ILLEGAL on failure
 *  if param is 1, value is encoded as a query parameter.
 */
static wg_int encode_pyobject_date(wg_database *db, PyObject *data,
  int param) {
  int datedata = wg_ymd_to_date(db->db,
    PyDateTime_GET_YEAR(data),
    PyDateTime_GET_MONTH(data),
    PyDateTime_GET_DAY(data));
  if(datedata > 0) {
    if(!param) {
      return wg_encode_date(db->db, datedata);
    } else {
      return wg_encode_query_param_date(db->db, datedata);
    }
  } else {
    return WG_ILLEGAL;
  }
}

/** Encode an atomic value of type WG_TIMETYPE
 *  returns WG_ILLEGAL on failure
 *  if param is 1, value is encoded as a query parameter.
 */
static wg_int encode_pyobject_time(wg_database *db, PyObject *data,
  int param) {
  int timedata = wg_hms_to_time(db->db,
    PyDateTime_TIME_GET_HOUR(data),
    PyDateTime_TIME_GET_MINUTE(data),
    PyDateTime_TIME_GET_SECOND(data),
    PyDateTime_TIME_GET_MICROSECOND(data)/10000);
  if(timedata >= 0) {
    if(!param) {
      return wg_encode_time(db->db, timedata);
    } else {
      return wg_encode_query_param_time(db->db, timedata);
    }
  } else {
    return WG_ILLEGAL;
  }
}

/** Encode an atomic value of type WG_VARTYPE
 *  returns WG_ILLEGAL on failure
 *  if param is 1, value is encoded as a query parameter.
 */
static wg_int encode_pyobject_var(wg_database *db, PyObject *data,
  int param) {
  int intdata;
#ifndef PYTHON3
  intdata = (int) PyInt_AsLong(data);
#else
  intdata = (int) PyLong_AsLong(data);
#endif

  if(!param) {
    return wg_encode_var(db->db, intdata);
  } else {
    return wg_encode_query_param_var(db->db, intdata);
  }
}

/** Encode Python object as wgdb value of specific type.
 *  returns WG_ILLEGAL if the conversion is not possible. The
 *  database API may also return WG_ILLEGAL.
 */
static wg_int encode_pyobject(wg_database *db, PyObject *data,
  wg_int ftype, char *ext_str, int param) {

  switch(ftype) {
    case WG_NULLTYPE:
      return encode_pyobject_null(db);
    case WG_RECORDTYPE:
      return encode_pyobject_record(db, data);
    case WG_INTTYPE:
      return encode_pyobject_int(db, data, param);
    case WG_DOUBLETYPE:
      return encode_pyobject_double(db, data, param);
    case WG_STRTYPE:
      return encode_pyobject_str(db, data, ext_str, param);
    case WG_URITYPE:
      return encode_pyobject_uri(db, data, ext_str, param);
    case WG_XMLLITERALTYPE:
      return encode_pyobject_xmlliteral(db, data, ext_str, param);
    case WG_CHARTYPE:
      return encode_pyobject_char(db, data, param);
    case WG_FIXPOINTTYPE:
      return encode_pyobject_fixpoint(db, data, param);
    case WG_DATETYPE:
      return encode_pyobject_date(db, data, param);
    case WG_TIMETYPE:
      return encode_pyobject_time(db, data, param);
    case WG_VARTYPE:
      return encode_pyobject_var(db, data, param);
    default:
      break;
  }

  /* Handle unknown type */
  return WG_ILLEGAL;
}

/** Encode Python object as wgdb value
 *  The object may be an immediate value or a tuple containing
 *  extended type information.
 *
 *  Conversion rules:
 *  Immediate Python value -> use the default wgdb type
 *  (value, ftype) -> use the provided field type (if possible)
 *  (value, ftype, ext_str) -> use the field type and extra string
 */
static wg_int encode_pyobject_ext(PyObject *self,
                            wg_database *db, PyObject *obj, int param) {
  PyObject *data;
  wg_int enc, ftype = 0;
  char *ext_str = NULL;

  if(PyTuple_Check(obj)) {
    /* Extended value. */
    int extargs = PyTuple_Size(obj);
    if(extargs<1 || extargs>3) {
      PyErr_SetString(PyExc_ValueError,
        "Values with extended type info must be 2/3-tuples.");
      return WG_ILLEGAL;
    }

    data = PyTuple_GetItem(obj, 0);

    if(extargs > 1) {
#ifndef PYTHON3
      ftype = (wg_int) PyInt_AsLong(PyTuple_GetItem(obj, 1));
#else
      ftype = (wg_int) PyLong_AsLong(PyTuple_GetItem(obj, 1));
#endif
      if(ftype<0) {
        PyErr_SetString(PyExc_ValueError,
          "Invalid field type for value.");
        return WG_ILLEGAL;
      }
    }

    if(extargs > 2) {
#ifndef PYTHON3
      ext_str = PyString_AsString(PyTuple_GetItem(obj, 2));
#elif defined(HAVE_LOCALEENC)
      ext_str = PyBytes_AsString(
        PyUnicode_EncodeLocale(PyTuple_GetItem(obj, 2), ENCODEERR));
#else
      ext_str = PyBytes_AsString(
        PyUnicode_AsEncodedString(PyTuple_GetItem(obj, 2), NULL, ENCODEERR));
#endif
      if(!ext_str) {
        /* Error has been set in conversion */
        return WG_ILLEGAL;
      }
    }
  } else {
    data = obj;
  }

  /* Now do the actual conversion. */
  ftype = pytype_to_wgtype(data, ftype);
  if(ftype == -1) {
    PyErr_SetString(PyExc_TypeError,
      "Requested encoding is not supported.");
    return WG_ILLEGAL;
  }
  else if(ftype == -2) {
    PyErr_SetString(PyExc_TypeError,
      "Value is of unsupported type.");
    return WG_ILLEGAL;
  }

  /* Now encode the given obj using the selected type */
  enc = encode_pyobject(db, data, ftype, ext_str, param);
  if(enc==WG_ILLEGAL)
    wgdb_error_setstring(self, "Value encoding error.");

  return enc;
}


/** Update field data.
 *  Data types supported:
 *  Python None. Translates to WhiteDB NULL (empty) field.
 *  Python integer.
 *  Python float.
 *  Python string. Embedded \0 bytes are not allowed (i.e. \0 is
 *  treated as a standard string terminator).
 *  XXX: add language support for str type?
 *  wgdb.Record object
 *  datetime.date object
 *  datetime.time object
 */

static PyObject *wgdb_set_field(PyObject *self, PyObject *args,
                                        PyObject *kwds) {
  PyObject *db = NULL, *rec = NULL;
  wg_int fieldnr, fdata = WG_ILLEGAL, err = 0, ftype = 0;
  PyObject *data;
  char *ext_str = NULL;
  static char *kwlist[] = {"db", "rec", "fieldnr", "data",
    "encoding", "ext_str", NULL};

  if(!PyArg_ParseTupleAndKeywords(args, kwds, "O!O!nO|ns", kwlist,
      &wg_database_type, &db, &wg_record_type, &rec,
      &fieldnr, &data, &ftype, &ext_str))
    return NULL;

  /* Determine the argument type. If the optional encoding
   * argument is not supplied, default encoding for the Python type
   * of the data is selected. Otherwise the user-provided encoding
   * is used, with the limitation that the Python type must
   * be compatible with the encoding.
   */
  ftype = pytype_to_wgtype(data, ftype);
  if(ftype == -1) {
    PyErr_SetString(PyExc_TypeError,
      "Requested encoding is not supported.");
    return NULL;
  }
  else if(ftype == -2) {
    PyErr_SetString(PyExc_TypeError,
      "Argument is of unsupported type.");
    return NULL;
  }

  /* Now encode the given data using the selected type */
  fdata = encode_pyobject((wg_database *) db, data, ftype, ext_str, 0);
  if(fdata==WG_ILLEGAL) {
    wgdb_error_setstring(self, "Field data conversion error.");
    return NULL;
  }

  /* Store the encoded field data in the record */
  err = wg_set_field(((wg_database *) db)->db,
    ((wg_record *) rec)->rec, fieldnr, fdata);
  if(err < 0) {
    wgdb_error_setstring(self, "Failed to set field value.");
    return NULL;
  }

  Py_INCREF(Py_None);
  return Py_None;
}

/** Set field data (assumes no previous content)
 *  Skips some bookkeeping related to the previous field
 *  contents, making the insert faster. Using it on fields
 *  that have previous content is likely to corrupt the database.
 *  Otherwise identical to set_field().
 */

static PyObject *wgdb_set_new_field(PyObject *self, PyObject *args,
                                        PyObject *kwds) {
  PyObject *db = NULL, *rec = NULL;
  wg_int fieldnr, fdata = WG_ILLEGAL, err = 0, ftype = 0;
  PyObject *data;
  char *ext_str = NULL;
  static char *kwlist[] = {"db", "rec", "fieldnr", "data",
    "encoding", "ext_str", NULL};

  if(!PyArg_ParseTupleAndKeywords(args, kwds, "O!O!nO|ns", kwlist,
      &wg_database_type, &db, &wg_record_type, &rec,
      &fieldnr, &data, &ftype, &ext_str))
    return NULL;

  ftype = pytype_to_wgtype(data, ftype);
  if(ftype == -1) {
    PyErr_SetString(PyExc_TypeError,
      "Requested encoding is not supported.");
    return NULL;
  }
  else if(ftype == -2) {
    PyErr_SetString(PyExc_TypeError,
      "Argument is of unsupported type.");
    return NULL;
  }

  fdata = encode_pyobject((wg_database *) db, data, ftype, ext_str, 0);
  if(fdata==WG_ILLEGAL) {
    wgdb_error_setstring(self, "Field data conversion error.");
    return NULL;
  }

  err = wg_set_new_field(((wg_database *) db)->db,
    ((wg_record *) rec)->rec, fieldnr, fdata);
  if(err < 0) {
    wgdb_error_setstring(self, "Failed to set field value.");
    return NULL;
  }

  Py_INCREF(Py_None);
  return Py_None;
}

/** Get decoded field value.
 *  Currently supported types:
 *   NULL - Python None
 *   record - wgdb.Record
 *   int - Python int
 *   double - Python float
 *   string - Python string
 *   char - Python string
 *   fixpoint - Python float
 *   date - datetime.date
 *   time - datetime.time
 */

static PyObject *wgdb_get_field(PyObject *self, PyObject *args) {
  PyObject *db = NULL, *rec = NULL;
  wg_int fieldnr, fdata, ftype;

  if(!PyArg_ParseTuple(args, "O!O!n", &wg_database_type, &db,
      &wg_record_type, &rec, &fieldnr))
    return NULL;

  /* First retrieve the field data. The information about
   * the field type is encoded inside the field.
   */
  fdata = wg_get_field(((wg_database *) db)->db,
    ((wg_record *) rec)->rec, fieldnr);
  if(fdata==WG_ILLEGAL) {
    wgdb_error_setstring(self, "Failed to get field data.");
    return NULL;
  }

  /* Decode the type */
  ftype = wg_get_encoded_type(((wg_database *) db)->db, fdata);
  if(!ftype) {
    wgdb_error_setstring(self, "Failed to get field type.");
    return NULL;
  }

  /* Decode (or retrieve) the actual data */
  if(ftype==WG_NULLTYPE) {
    Py_INCREF(Py_None);
    return Py_None;
  }
  else if(ftype==WG_RECORDTYPE) {
    wg_record *ddata = (wg_record *) wg_record_type.tp_alloc(
      &wg_record_type, 0);
    if(!ddata) return NULL;

    ddata->rec = wg_decode_record(((wg_database *) db)->db, fdata);
    if(!ddata->rec) {
      wgdb_error_setstring(self, "Failed to fetch a record.");
      wg_record_type.tp_free(ddata);
      return NULL;
    }
    Py_INCREF(ddata);
    return (PyObject *) ddata;
  }
  else if(ftype==WG_INTTYPE) {
    wg_int ddata = wg_decode_int(((wg_database *) db)->db, fdata);
    return Py_BuildValue("n", ddata);
  }
  else if(ftype==WG_DOUBLETYPE) {
    double ddata = wg_decode_double(((wg_database *) db)->db, fdata);
    return Py_BuildValue("d", ddata);
  }
  else if(ftype==WG_STRTYPE) {
    char *ddata = wg_decode_str(((wg_database *) db)->db, fdata);
    /* Data is copied here, no leaking */
    return Py_BuildValue("s", ddata);
  }
  else if(ftype==WG_URITYPE) {
    char *ddata = wg_decode_uri(((wg_database *) db)->db, fdata);
    char *ext_str = wg_decode_uri_prefix(((wg_database *) db)->db, fdata);
    if(ext_str) {
#ifndef PYTHON3
      return PyString_FromFormat("%s%s", ext_str, ddata);
#else
      return PyUnicode_FromFormat("%s%s", ext_str, ddata);
#endif
    }
    else return Py_BuildValue("s", ddata);
  }
  else if(ftype==WG_XMLLITERALTYPE) {
    char *ddata = wg_decode_xmlliteral(((wg_database *) db)->db, fdata);
    return Py_BuildValue("s", ddata);
  }
  else if(ftype==WG_CHARTYPE) {
    char ddata[2];
    ddata[0] = wg_decode_char(((wg_database *) db)->db, fdata);
    ddata[1] = '\0';
    return Py_BuildValue("s", ddata); /* treat as single-character string */
  }
  else if(ftype==WG_FIXPOINTTYPE) {
    double ddata = wg_decode_fixpoint(((wg_database *) db)->db, fdata);
    return Py_BuildValue("d", ddata);
  }
  else if(ftype==WG_DATETYPE) {
    int year, month, day;
    int datedata = wg_decode_date(((wg_database *) db)->db, fdata);

    if(!datedata) {
      wgdb_error_setstring(self, "Failed to decode date.");
      return NULL;
    }
    wg_date_to_ymd(((wg_database *) db)->db, datedata, &year, &month, &day);
    return PyDate_FromDate(year, month, day);
  }
  else if(ftype==WG_TIMETYPE) {
    int hour, minute, second, fraction;
    int timedata = wg_decode_time(((wg_database *) db)->db, fdata);
    /* 0 is both a valid time of 00:00:00.00 and an error. So the
     * latter case is ignored here. */
    wg_time_to_hms(((wg_database *) db)->db, timedata,
      &hour, &minute, &second, &fraction);
    return PyTime_FromTime(hour, minute, second, fraction*10000);
  }
  else if(ftype==WG_VARTYPE) {
    /* XXX: returns something that, if written back to database
     * unaltered, will result in the database field actually containing
     * the same variable. The literal value in Python is not very meaningful,
     * however this approach preserves consistency in handling the type
     * conversions.
     */
    int ddata = wg_decode_var(((wg_database *) db)->db, fdata);
    return Py_BuildValue("(i,i)", ddata, WG_VARTYPE);
  }
  else {
    char buf[80];
    snprintf(buf, 80, "Cannot handle field type %d.", (int) ftype);
    wgdb_error_setstring(self, buf);
    return NULL;
  }
}

/*
 * Functions to handle transactions. Logical level of
 * concurrency control with wg_start_write() and friends
 * is implemented here. In the simplest case, these functions
 * internally map to physical locking and unlocking, however they
 * should not be relied upon to do so.
 */

/** Start a writing transaction
 *  Python wrapper to wg_start_write()
 *  Returns lock id when successful, otherwise raises an exception.
 */

static PyObject * wgdb_start_write(PyObject *self, PyObject *args) {
  PyObject *db = NULL;
  wg_int lock_id = 0;

  if(!PyArg_ParseTuple(args, "O!", &wg_database_type, &db))
    return NULL;

  lock_id = wg_start_write(((wg_database *) db)->db);
  if(!lock_id) {
    wgdb_error_setstring(self, "Failed to acquire write lock.");
    return NULL;
  }

  return Py_BuildValue("i", (int) lock_id);
}

/** Finish a writing transaction
 *  Python wrapper to wg_end_write()
 *  Returns None when successful, otherwise raises an exception.
 */

static PyObject * wgdb_end_write(PyObject *self, PyObject *args) {
  PyObject *db = NULL;
  wg_int lock_id = 0;

  if(!PyArg_ParseTuple(args, "O!n", &wg_database_type, &db, &lock_id))
    return NULL;

  if(!wg_end_write(((wg_database *) db)->db, lock_id)) {
    wgdb_error_setstring(self, "Failed to release write lock.");
    return NULL;
  }

  Py_INCREF(Py_None);
  return Py_None;
}

/** Start a reading transaction
 *  Python wrapper to wg_start_read()
 *  Returns lock id when successful, otherwise raises an exception.
 */

static PyObject * wgdb_start_read(PyObject *self, PyObject *args) {
  PyObject *db = NULL;
  wg_int lock_id = 0;

  if(!PyArg_ParseTuple(args, "O!", &wg_database_type, &db))
    return NULL;

  lock_id = wg_start_read(((wg_database *) db)->db);
  if(!lock_id) {
    wgdb_error_setstring(self, "Failed to acquire read lock.");
    return NULL;
  }

  return Py_BuildValue("i", (int) lock_id);
}

/** Finish a reading transaction
 *  Python wrapper to wg_end_read()
 *  Returns None when successful, otherwise raises an exception.
 */

static PyObject * wgdb_end_read(PyObject *self, PyObject *args) {
  PyObject *db = NULL;
  wg_int lock_id = 0;

  if(!PyArg_ParseTuple(args, "O!n", &wg_database_type, &db, &lock_id))
    return NULL;

  if(!wg_end_read(((wg_database *) db)->db, lock_id)) {
    wgdb_error_setstring(self, "Failed to release read lock.");
    return NULL;
  }

  Py_INCREF(Py_None);
  return Py_None;
}

/* Functions to create and fetch data from queries.
 * The query object defined on wgdb module level stores both
 * the pointer to the query and all the encoded parameters -
 * since the parameters use database side storage, they
 * should preferrably be freed after the query is finished.
 */

/** Parse query arguments.
 * Creates wgdb query arglist and matchrec.
 */

static int parse_query_params(PyObject *self, PyObject *args,
                            PyObject *kwds, wg_query_ob *query) {
  PyObject *db = NULL;
  PyObject *arglist = NULL;
  PyObject *matchrec = NULL;
  static char *kwlist[] = {"db", "matchrec", "arglist", NULL};

  if(!PyArg_ParseTupleAndKeywords(args, kwds, "O!|OO", kwlist,
      &wg_database_type, &db, &matchrec, &arglist))
    return 0;

  Py_INCREF(db);  /* Make sure we don't lose database connection */
  query->db = (wg_database *) db;

  /* Determine type of arglist */
  query->argc = 0;
  if(arglist && arglist!=Py_None) {
    int len, i;
    if(!PySequence_Check(arglist)) {
      PyErr_SetString(PyExc_TypeError, "Query arglist must be a sequence.");
      return 0;
    }
    len = (int) PySequence_Size(arglist);

    /* Argument list was present. Extract the individual arguments. */
    if(len > 0) {
      query->arglist = (wg_query_arg *) malloc(len * sizeof(wg_query_arg));
      if(!query->arglist) {
        wgdb_error_setstring(self, "Failed to allocate memory.");
        return 0;
      }
      memset(query->arglist, 0, len * sizeof(wg_query_arg));

      /* Now copy all the parameters. */
      for(i=0; i<len; i++) {
        int col, cond;
        wg_int enc;
        PyObject *t = PySequence_GetItem(arglist, (Py_ssize_t) i);

        if(!PyTuple_Check(t) || PyTuple_Size(t) != 3) {
          PyErr_SetString(PyExc_ValueError,
            "Query arglist item must be a 3-tuple.");
          Py_DECREF(t);
          return 0;
        }

#ifndef PYTHON3
        col = PyInt_AsLong(PyTuple_GetItem(t, 0));
#else
        col = PyLong_AsLong(PyTuple_GetItem(t, 0));
#endif
        if(col==-1 && PyErr_Occurred()) {
          PyErr_SetString(PyExc_ValueError,
            "Failed to convert query argument column number.");
          Py_DECREF(t);
          return 0;
        }

#ifndef PYTHON3
        cond = PyInt_AsLong(PyTuple_GetItem(t, 1));
#else
        cond = PyLong_AsLong(PyTuple_GetItem(t, 1));
#endif
        if(cond==-1 && PyErr_Occurred()) {
          PyErr_SetString(PyExc_ValueError,
            "Failed to convert query argument condition.");
          Py_DECREF(t);
          return 0;
        }

        enc = encode_pyobject_ext(self, query->db, PyTuple_GetItem(t, 2), 1);
        if(enc==WG_ILLEGAL) {
          /* Error set by encode function */
          Py_DECREF(t);
          return 0;
        }

        /* Finally, set the argument fields */
        query->arglist[i].column = col;
        query->arglist[i].cond = cond;
        query->arglist[i].value = enc;

        query->argc++; /* We have successfully encoded a parameter. We're
                        * not setting this to len immediately, because
                        * there might be an encoding error and part of
                        * the arguments may be left uninitialized. */
        Py_DECREF(t);
      }
    }
  }

  query->reclen = 0;
  /* Determine type of matchrec */
  if(matchrec && matchrec!=Py_None) {
    if(PyObject_TypeCheck(matchrec, &wg_record_type)) {
      /* Database record pointer was given. Pass it directly
       * to the query.
       */
      query->matchrec = ((wg_record *) matchrec)->rec;
    }
    else if(PySequence_Check(matchrec)) {
      int len = (int) PySequence_Size(matchrec);
      if(len) {
        int i;

        /* Construct the record. */
        query->matchrec = malloc(len * sizeof(wg_int));
        if(!query->matchrec) {
          wgdb_error_setstring(self, "Failed to allocate memory.");
          return 0;
        }
        memset(query->matchrec, 0, len * sizeof(wg_int));

        for(i=0; i<len; i++) {
          PyObject *sg_t = PySequence_GetItem(matchrec, i);
          wg_int enc = encode_pyobject_ext(self, query->db, sg_t, 1);
          Py_DECREF(sg_t);
          if(enc==WG_ILLEGAL) {
            /* Error set by encode function */
            return 0;
          }

          ((wg_int *) query->matchrec)[i] = enc;
          query->reclen++; /* Count the successfully encoded fields. */
        }
      }
    }
    else {
      PyErr_SetString(PyExc_TypeError,
        "Query match record must be a sequence or a wgdb.Record");
      return 0;
    }
  }

  return 1;
}


/** Create a query object.
 *  Python wrapper to wg_make_query()
 */

static PyObject * wgdb_make_query(PyObject *self, PyObject *args,
                                        PyObject *kwds) {
  wg_query_ob *query;

  /* Build a new query object */
  query = (wg_query_ob *) wg_query_type.tp_alloc(&wg_query_type, 0);
  if(!query) return NULL;

  query->query = NULL;
  query->db = NULL;
  query->arglist = NULL;
  query->argc = 0;
  query->matchrec = NULL;
  query->reclen = 0;

  /* Create the arglist and matchrec from parameters. */
  if(!parse_query_params(self, args, kwds, query)) {
    wg_query_dealloc(query);
    return NULL;
  }

  query->query = wg_make_query(query->db->db, query->matchrec, query->reclen,
    query->arglist, query->argc);

  if(!query->query) {
    wgdb_error_setstring(self, "Failed to create the query.");
    /* Call destructor. It should take care of all the allocated memory. */
    wg_query_dealloc(query);
    return NULL;
  }
  return (PyObject *) query;
}

/** Fetch next row from a query.
 *  Python wrapper for wg_fetch()
 */

static PyObject * wgdb_fetch(PyObject *self, PyObject *args) {
  PyObject *db = NULL, *query = NULL;
  wg_record *rec;

  if(!PyArg_ParseTuple(args, "O!O!", &wg_database_type, &db,
      &wg_query_type, &query))
    return NULL;

  /* Build a new record object */
  rec = (wg_record *) wg_record_type.tp_alloc(&wg_record_type, 0);
  if(!rec) return NULL;

  rec->rec = wg_fetch(((wg_database *) db)->db,
    ((wg_query_ob *) query)->query);
  if(!rec->rec) {
    wgdb_error_setstring(self, "Failed to fetch a record.");
    wg_record_type.tp_free(rec);
    return NULL;
  }

  Py_INCREF(rec);
  return (PyObject *) rec;
}

/** Free query.
 *  Python wrapper to wg_free_query()
 *  In addition, this function frees the local memory for
 *  the arguments and attempts to free database-side encoded data.
 */

static PyObject * wgdb_free_query(PyObject *self, PyObject *args) {
  PyObject *db = NULL, *query = NULL;

  if(!PyArg_ParseTuple(args, "O!O!", &wg_database_type, &db,
      &wg_query_type, &query))
    return NULL;

  /* XXX: since the query contains the db pointer, ignore
   * the database object we were given (it is still required
   * for consistency between the API-s and possible future
   * extensions).
   */
  free_query((wg_query_ob *) query);
  Py_INCREF(Py_None);
  return Py_None;
}

/** Helper function to free local query memory
 *  (wg_query_ob *) query->db field is used as a marker
 *  (set to NULL for queries that do not need deallocating).
 */
static void free_query(wg_query_ob *obj) {
  if(obj->db) {
#if 0 /* Suppress the warning if we use local parameters */
    if(!obj->db->db) {
      fprintf(stderr,
        "Warning: database connection lost before freeing encoded data\n");
    }
#endif
    /* Allow freeing the query object.
     * XXX: this is hacky. db pointer may become significant
     * in the future, which makes this a timebomb.
     */
    if(obj->query)
      wg_free_query(obj->db->db, obj->query);

    if(obj->arglist) {
      if(obj->db->db) {
        int i;
        for(i=0; i<obj->argc; i++)
          wg_free_query_param(obj->db->db, obj->arglist[i].value);
      }
      free(obj->arglist);
    }
    if(obj->matchrec && obj->reclen) {
      if(obj->db->db) {
        int i;
        for(i=0; i<obj->reclen; i++)
          wg_free_query_param(obj->db->db, ((wg_int *) obj->matchrec)[i]);
      }
      free(obj->matchrec);
    }

    Py_DECREF(obj->db);
    obj->db = NULL;
  }
}

/* Methods for data types defined by this module.
 */

/** Database object desctructor.
 * Detaches from shared memory or frees local memory.
 */
static void wg_database_dealloc(wg_database *obj) {
  if(obj->db) {
    if(obj->local)
      wg_delete_local_database(obj->db);
    else
      wg_detach_database(obj->db);
  }
#ifndef PYTHON3
  obj->ob_type->tp_free((PyObject *) obj);
#else
  Py_TYPE(obj)->tp_free((PyObject *) obj);
#endif
}

/** Query object desctructor.
 * Frees query and encoded query parameters.
 */
static void wg_query_dealloc(wg_query_ob *obj) {
  free_query(obj);
#ifndef PYTHON3
  obj->ob_type->tp_free((PyObject *) obj);
#else
  Py_TYPE(obj)->tp_free((PyObject *) obj);
#endif
}

/** String representation of database object. This is used for both
 * repr() and str()
 */
static PyObject *wg_database_repr(wg_database * obj) {
  /* XXX: this is incompatible with eval(). If a need to
   * eval() such representations should arise, new initialization
   * function is also needed for the type.
   */
#ifndef PYTHON3
  return PyString_FromFormat("<WhiteDB database at %p>",
    (void *) obj->db);
#else
  return PyUnicode_FromFormat("<WhiteDB database at %p>",
    (void *) obj->db);
#endif
}

/** String representation of record object. This is used for both
 * repr() and str()
 */
static PyObject *wg_record_repr(wg_record * obj) {
  /* XXX: incompatible with eval(). */
#ifndef PYTHON3
  return PyString_FromFormat("<WhiteDB record at %p>",
    (void *) obj->rec);
#else
  return PyUnicode_FromFormat("<WhiteDB record at %p>",
    (void *) obj->rec);
#endif
}

/** String representation of query object. Used for both repr() and str()
 */
static PyObject *wg_query_repr(wg_query_ob *obj) {
  /* XXX: incompatible with eval(). */
#ifndef PYTHON3
  return PyString_FromFormat("<WhiteDB query at %p>",
    (void *) obj->query);
#else
  return PyUnicode_FromFormat("<WhiteDB query at %p>",
    (void *) obj->query);
#endif
}

/** Get the number of rows in a query result
 */
static PyObject *wg_query_get_res_count(wg_query_ob *obj, void *closure) {
  if(obj->query) {
    if(obj->query->qtype == WG_QTYPE_PREFETCH) {
      return Py_BuildValue("K", (unsigned PY_LONG_LONG) obj->query->res_count);
    } else {
      return Py_None;
    }
  } else {
    PyErr_SetString(PyExc_ValueError, "Invalid query object");
    return NULL;
  }
  return Py_None; /* satisfy the compiler */
}

/** Set the number of rows in a query result (not allowed)
 */
static int wg_query_set_res_count(wg_query_ob *obj,
                                      PyObject *value, void *closure) {
  PyErr_SetString(PyExc_AttributeError, "res_count is a read only attribute");
  return -1;
}

/** Set module exception.
 *
 */
static void wgdb_error_setstring(PyObject *self, char *err) {
#ifndef PYTHON3
  struct module_state *st = &_state;
#else
  struct module_state *st = (struct module_state *) PyModule_GetState(self);
#endif
  PyErr_SetString(st->wgdb_error, err);
}

/** Initialize module.
 *  Standard entry point for Python extension modules, executed
 *  during import.
 */
#ifdef PYTHON3
#define INITERROR return NULL;
#else
#define INITERROR return;
#endif

#ifndef PYTHON3
PyMODINIT_FUNC initwgdb(void)
#else
PyMODINIT_FUNC PyInit_wgdb(void)
#endif
{
  PyObject *m;
  struct module_state *st;

  wg_database_type.tp_new = PyType_GenericNew;
  if (PyType_Ready(&wg_database_type) < 0)
    INITERROR

  wg_record_type.tp_new = PyType_GenericNew;
  if (PyType_Ready(&wg_record_type) < 0)
    INITERROR

  wg_query_type.tp_new = PyType_GenericNew;
  if (PyType_Ready(&wg_query_type) < 0)
    INITERROR

#ifndef PYTHON3
  m = Py_InitModule3("wgdb", wgdb_methods, "WhiteDB database adapter");
#else
  m = PyModule_Create(&wgdb_def);
#endif
  if(!m) INITERROR

#ifndef PYTHON3
  st = &_state;
#else
  st = (struct module_state *) PyModule_GetState(m);
#endif
  st->wgdb_error = PyErr_NewException("wgdb.error", NULL, NULL);
  Py_INCREF(st->wgdb_error);
  PyModule_AddObject(m, "error", st->wgdb_error);

  /* Expose wgdb internal encoding types */
  PyModule_AddIntConstant(m, "NULLTYPE", WG_NULLTYPE);
  PyModule_AddIntConstant(m, "RECORDTYPE", WG_RECORDTYPE);
  PyModule_AddIntConstant(m, "INTTYPE", WG_INTTYPE);
  PyModule_AddIntConstant(m, "DOUBLETYPE", WG_DOUBLETYPE);
  PyModule_AddIntConstant(m, "STRTYPE", WG_STRTYPE);
  PyModule_AddIntConstant(m, "XMLLITERALTYPE", WG_XMLLITERALTYPE);
  PyModule_AddIntConstant(m, "URITYPE", WG_URITYPE);
  PyModule_AddIntConstant(m, "BLOBTYPE", WG_BLOBTYPE);
  PyModule_AddIntConstant(m, "CHARTYPE", WG_CHARTYPE);
  PyModule_AddIntConstant(m, "FIXPOINTTYPE", WG_FIXPOINTTYPE);
  PyModule_AddIntConstant(m, "DATETYPE", WG_DATETYPE);
  PyModule_AddIntConstant(m, "TIMETYPE", WG_TIMETYPE);
  PyModule_AddIntConstant(m, "VARTYPE", WG_VARTYPE);

/* these types are not implemented yet:
  PyModule_AddIntConstant(m, "ANONCONSTTYPE", WG_ANONCONSTTYPE);
*/

  /* Expose query conditions */
  PyModule_AddIntConstant(m, "COND_EQUAL", WG_COND_EQUAL);
  PyModule_AddIntConstant(m, "COND_NOT_EQUAL", WG_COND_NOT_EQUAL);
  PyModule_AddIntConstant(m, "COND_LESSTHAN", WG_COND_LESSTHAN);
  PyModule_AddIntConstant(m, "COND_GREATER", WG_COND_GREATER);
  PyModule_AddIntConstant(m, "COND_LTEQUAL", WG_COND_LTEQUAL);
  PyModule_AddIntConstant(m, "COND_GTEQUAL", WG_COND_GTEQUAL);

  /* Initialize PyDateTime C API */
  PyDateTime_IMPORT;
#ifdef PYTHON3
  return m;
#endif
}
