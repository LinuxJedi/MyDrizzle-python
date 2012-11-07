/*
Copyright 2012 Hewlett-Packard Development Company, L.P.

Licensed under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain
a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.
*/

#include <Python.h>
#include <bytesobject.h>
#include <structmember.h>
#include <libdrizzle/drizzle_client.h>
#include <stdint.h>

static PyObject *_drizzle_DrizzleError;
static PyObject *_drizzle_Warning;
static PyObject *_drizzle_Error;
static PyObject *_drizzle_DatabaseError;
static PyObject *_drizzle_InterfaceError;
static PyObject *_drizzle_DataError;
static PyObject *_drizzle_OperationalError;
static PyObject *_drizzle_IntegrityError;
static PyObject *_drizzle_InternalError;
static PyObject *_drizzle_ProgrammingError;
static PyObject *_drizzle_NotSupportedError;
static PyObject *_drizzle_NULL;

typedef struct {
    PyObject_HEAD
    drizzle_con_st *con;
    int open;
    PyObject *converter;
} _drizzle_ConnectionObject;

static char _drizzle_connect__doc__[] =
"Returns a Drizzle connection object.\n\
\n\
host\n\
    string, host to connect\n\
";

static int _drizzle_ConnectionObject_traverse(
        _drizzle_ConnectionObject *self,
        visitproc visit,
        void *arg)
{
    if (self->converter)
    {
        return visit(self->converter, arg);
    }
    return 0;
}

static int _drizzle_ConnectionObject_clear(
        _drizzle_ConnectionObject *self)
{
    Py_XDECREF(self->converter);
    self->converter= NULL;
    return 0;
}

extern PyTypeObject _drizzle_ConnectionObject_Type;

static PyObject *_drizzle_NewException(
        PyObject *dict,
        PyObject *edict,
        char *name)
{
    PyObject *e;

    if (!(e = PyDict_GetItemString(edict, name)))
    {
        return NULL;
    }
    if (PyDict_SetItemString(dict, name, e))
    {
        return NULL;
    }
    Py_INCREF(e);
    return e;
}

PyObject *_drizzle_Exception(_drizzle_ConnectionObject *d)
{
    PyObject *t, *e;
    if (!(t = PyTuple_New(2)))
    {
        return NULL;
    }
    e = _drizzle_InternalError;
    PyTuple_SET_ITEM(t, 0, PyInt_FromLong(-1L));
    PyTuple_SET_ITEM(t, 1, PyString_FromString("generic error"));
    PyErr_SetObject(e, t);
    Py_DECREF(t);
    return NULL;
}

static int _drizzle_ConnectionObject_Initialize(
        _drizzle_ConnectionObject *self,
        PyObject *args,
        PyObject *kwargs)
{
    drizzle_st *drizzle= NULL;
    drizzle_con_st *con= NULL;
    drizzle_return_t ret;
    PyObject *conv = NULL;
    char *host= NULL, *user= NULL, *passwd= NULL, *db= NULL,
         *unix_socket= NULL, *init_command= NULL;
    uint16_t port= 0;
    int connect_timeout= 0, client_flag= 0;
    static char *kwlist[]= { "host", "user", "passwd", "db", "port",
        "conv", "unix_socket", "connect_timeout", "init_command",
        "client_flag", NULL };

    self->open= 0;
    self->converter= NULL;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|ssssHOsisi:connect",
                kwlist,
                &host, &user, &passwd, &db, &port, &conv, &unix_socket,
                &connect_timeout, &init_command, &client_flag))
    {
        return -1;
    }

    drizzle = drizzle_create(NULL);
    con = drizzle_con_create(drizzle, NULL);

    if (unix_socket != NULL)
    {
        drizzle_con_set_uds(con, unix_socket);
    }
    else
    {
        drizzle_con_set_tcp(con, host, port);
    }
    drizzle_con_set_auth(con, user, passwd);
    drizzle_con_set_db(con, db);
    drizzle_con_add_options(con, client_flag);

    ret = drizzle_con_connect(con);

    if (ret != DRIZZLE_RETURN_OK)
    {
        _drizzle_Exception(self);
        return -1;
    }
    if (!conv)
    {
        conv= PyDict_New();
    }
    else
    {
        Py_INCREF(conv);
    }
    if (!conv)
    {
        return -1;
    }
    self->converter = conv;
    self->con= con;
    self->open= 1;
    return 0;
}

static char _drizzle_ConnectionObject_close__doc__[] =
"Close the connection. No further activity possible.";

static PyObject *_drizzle_ConnectionObject_close(
        _drizzle_ConnectionObject *self,
        PyObject *args)
{
    if ((args) && (!PyArg_ParseTuple(args, "")))
    {
            return NULL;
    }
    if (self->open)
    {
        drizzle_con_close(self->con);
        self->open= 0;
    }
    else
    {
        PyErr_SetString(_drizzle_ProgrammingError,
                "closing a closed connection");
        return NULL;
    }
    _drizzle_ConnectionObject_clear(self);
    Py_INCREF(Py_None);
    return Py_None;
}

static void _drizzle_ConnectionObject_dealloc(
        _drizzle_ConnectionObject *self)
{
    PyObject *o;

    if (self->open)
    {
        o= _drizzle_ConnectionObject_close(self, NULL);
        Py_XDECREF(o);
    }
    PyObject_Del(self);
}

static PyObject *_drizzle_connect(
        PyObject *self,
        PyObject *args,
        PyObject *kwargs)
{
   _drizzle_ConnectionObject *d= NULL;
   d = PyObject_New(_drizzle_ConnectionObject, &_drizzle_ConnectionObject_Type);
   if (d == NULL)
   {
       return NULL;
   }
   if (_drizzle_ConnectionObject_Initialize(d, args, kwargs))
   {
       Py_DECREF(d);
       d = NULL;
   }
   return (PyObject *) d;
}

static struct PyMethodDef _drizzle_methods[] = {
    {
        "connect",
        (PyCFunction)_drizzle_connect,
        METH_VARARGS | METH_KEYWORDS,
        _drizzle_connect__doc__
    }
};

static struct PyMethodDef _drizzle_ConnectionObject_methods[] = {
    {
        "close",
        (PyCFunction)_drizzle_ConnectionObject_close,
        METH_VARARGS,
        _drizzle_ConnectionObject_close__doc__
    },
    {NULL, NULL}
};

static struct PyMemberDef _drizzle_ConnectionObject_memberlist[] = {
    {
        "open",
        T_INT,
        offsetof(_drizzle_ConnectionObject, open),
        READONLY,
        "True if connection is open"
    },
    {
        "converter",
        T_OBJECT,
        offsetof(_drizzle_ConnectionObject, converter),
        0,
        "Type conversion mapping"
    },
    {NULL}

};

static PyObject *_drizzle_ConnectionObject_getattr(
        _drizzle_ConnectionObject *self,
        char *name)
{
    if (strcmp(name, "closed") == 0)
    {
        return PyInt_FromLong((long)!(self->open));
    }
    PyMemberDef *l;
    for (l = _drizzle_ConnectionObject_memberlist; l->name != NULL; l++)
    {
        if (strcmp(l->name, name) == 0)
        {
            return PyMember_GetOne((char *)self, l);
        }
    }
    PyErr_SetString(PyExc_AttributeError, name);
    return NULL;
}

static PyObject *_drizzle_ConnectionObject_setattr(
        _drizzle_ConnectionObject *self,
        char *name,
        PyObject *v)
{
    if (v == NULL)
    {
        PyErr_SetString(PyExc_AttributeError,
                "can't delete connection attributes");
        return -1;
    }

    PyMemberDef *l;
    for (l = _drizzle_ConnectionObject_memberlist; l->name != NULL; l++)
    {
        if (strcmp(l->name, name) == 0)
        {
            return PyMember_SetOne((char *)self, l, v);
        }
    }
    PyErr_SetString(PyExc_AttributeError, name);
    return -1;
}

static PyObject *_drizzle_ConnectionObject_repr(
        _drizzle_ConnectionObject *self)
{
    char buf[256];
    if (self->open)
    {
        if (self->con->socket_type == DRIZZLE_CON_SOCKET_TCP)
        {
            sprintf(buf, "<_drizzle.connection open to '%.128s' at %lx>",
                    self->con->socket.tcp.host,
                    (long)self);
        }
        else
        {
            sprintf(buf, "<_drizzle.connection open to '%.128s' at %lx>",
                    self->con->socket.uds.path_buffer,
                    (long)self);
        }
    }
    else
    {
        sprintf(buf, "<_drizzle.connection closed at %lx>",
                (long)self);
    }
    return PyString_FromString(buf);
}

PyTypeObject _drizzle_ConnectionObject_Type = {
    PyObject_HEAD_INIT(NULL)
    0,
    "_drizzle.connection",
    sizeof(_drizzle_ConnectionObject),
    0,
    (destructor)_drizzle_ConnectionObject_dealloc,
    0,
    (getattrfunc)_drizzle_ConnectionObject_getattr,
    (setattrfunc)_drizzle_ConnectionObject_setattr,
    0,
    (reprfunc)_drizzle_ConnectionObject_repr,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_BASETYPE,
    _drizzle_connect__doc__,
    (traverseproc) _drizzle_ConnectionObject_traverse,
    (inquiry) _drizzle_ConnectionObject_clear,
    0,
    0,
    0,
    0,
    (struct PyMethodDef *)_drizzle_ConnectionObject_methods,
    (struct PyMemberDef *)_drizzle_ConnectionObject_memberlist,
    0,
    0,
    0,
    0,
    0,
    0,
    (initproc)_drizzle_ConnectionObject_Initialize,
    NULL,
    NULL,
    NULL,
    0,
    0,
    0,
};

static char _drizzle___doc__[] =
"The Drizzle API in Python form!";

DL_EXPORT(void) init_drizzle(void)
{
    PyObject *dict, *module, *emod, *edict;
    module = Py_InitModule4("_drizzle", _drizzle_methods, _drizzle___doc__,
            (PyObject *)NULL, PYTHON_API_VERSION);
    if (!module) return;
    _drizzle_ConnectionObject_Type.ob_type = &PyType_Type;
    _drizzle_ConnectionObject_Type.tp_alloc = PyType_GenericAlloc;
    _drizzle_ConnectionObject_Type.tp_new = PyType_GenericNew;
    if (!(dict = PyModule_GetDict(module))) goto error;
    if (PyDict_SetItemString(dict, "0.1",
            PyRun_String("0.1", Py_eval_input,
            dict, dict)))
    {
        goto error;
    }
    if (PyDict_SetItemString(dict, "0.1",
                PyString_FromString("0.1")))
    {
        goto error;
    }
    if (PyDict_SetItemString(dict, "connection",
                (PyObject *)&_drizzle_ConnectionObject_Type))
    {
        goto error;
    }
    Py_INCREF(&_drizzle_ConnectionObject_Type);

    if (!(emod = PyImport_ImportModule("_drizzle_exceptions"))) {
        PyErr_Print();
            goto error;
    }
    if (!(edict = PyModule_GetDict(emod))) goto error;
    if (!(_drizzle_DrizzleError =
          _drizzle_NewException(dict, edict, "DrizzleError")))
            goto error;
    if (!(_drizzle_Warning =
          _drizzle_NewException(dict, edict, "Warning")))
            goto error;
    if (!(_drizzle_Error =
          _drizzle_NewException(dict, edict, "Error")))
            goto error;
    if (!(_drizzle_InterfaceError =
          _drizzle_NewException(dict, edict, "InterfaceError")))
            goto error;
    if (!(_drizzle_DatabaseError =
          _drizzle_NewException(dict, edict, "DatabaseError")))
            goto error;
    if (!(_drizzle_DataError =
          _drizzle_NewException(dict, edict, "DataError")))
            goto error;
    if (!(_drizzle_OperationalError =
          _drizzle_NewException(dict, edict, "OperationalError")))
            goto error;
    if (!(_drizzle_IntegrityError =
          _drizzle_NewException(dict, edict, "IntegrityError")))
            goto error;
    if (!(_drizzle_InternalError =
          _drizzle_NewException(dict, edict, "InternalError")))
            goto error;
    if (!(_drizzle_ProgrammingError =
          _drizzle_NewException(dict, edict, "ProgrammingError")))
            goto error;
    if (!(_drizzle_NotSupportedError =
          _drizzle_NewException(dict, edict, "NotSupportedError")))
            goto error;
    Py_DECREF(emod);
    if (!(_drizzle_NULL = PyString_FromString("NULL")))
            goto error;
    if (PyDict_SetItemString(dict, "NULL", _drizzle_NULL)) goto error;
    error:
    if (PyErr_Occurred()) 
    {
            PyErr_SetString(PyExc_ImportError,
                            "_drizzle: init failed");
            module = NULL;
    }
}
