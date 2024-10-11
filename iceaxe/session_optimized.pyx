# cython_optimizations.pyx
from typing import Any, List, Tuple, Type
from iceaxe.base import TableBase
from iceaxe.queries import FunctionMetadata
from json import loads as json_loads
from cpython.ref cimport PyObject
from cpython.object cimport PyObject_GetItem
from libc.stdlib cimport malloc, free
from libc.string cimport strcpy

cdef struct FieldInfo:
    char* name
    bint is_json

cdef list optimize_casting(list values, list select_raws, list select_types):
    cdef:
        Py_ssize_t i, j, k, num_values, num_selects
        list result_all
        PyObject **result_value
        object value, obj, item
        tuple select_type
        bint raw_is_table, raw_is_column, raw_is_function_metadata
        FieldInfo **fields
        Py_ssize_t num_fields
        dict field_dict
        bytes field_bytes
        char* c_field_name

    num_values = len(values)
    num_selects = len(select_raws)
    result_all = [None] * num_values

    # Pre-calculate field information
    fields = <FieldInfo**>malloc(num_selects * sizeof(FieldInfo*))
    if not fields:
        raise MemoryError()

    try:
        for j in range(num_selects):
            select_raw = select_raws[j]
            raw_is_table, raw_is_column, raw_is_function_metadata = select_types[j]
            if raw_is_table:
                field_dict = {}
                for field, info in select_raw.model_fields.items():
                    if not info.exclude:
                        field_dict[field] = info.is_json
                num_fields = len(field_dict)
                fields[j] = <FieldInfo*>malloc(num_fields * sizeof(FieldInfo))
                if not fields[j]:
                    raise MemoryError()
                for k, (field, is_json) in enumerate(field_dict.items()):
                    field_bytes = field.encode('utf-8')
                    c_field_name = <char*>malloc((len(field_bytes) + 1) * sizeof(char))
                    if not c_field_name:
                        raise MemoryError()
                    strcpy(c_field_name, field_bytes)
                    fields[j][k].name = c_field_name
                    fields[j][k].is_json = is_json

        for i in range(num_values):
            value = values[i]
            result_value = <PyObject**>malloc(num_selects * sizeof(PyObject*))
            if not result_value:
                raise MemoryError()
            try:
                for j in range(num_selects):
                    select_raw = select_raws[j]
                    raw_is_table, raw_is_column, raw_is_function_metadata = select_types[j]
                    if raw_is_table:
                        obj_dict = {}
                        for k in range(num_fields):
                            field_name = fields[j][k].name.decode('utf-8')
                            field_value = PyObject_GetItem(value, field_name)
                            if fields[j][k].is_json:
                                field_value = json_loads(field_value)
                            obj_dict[field_name] = field_value
                        obj = select_raw(**obj_dict)
                        result_value[j] = <PyObject*>obj
                    elif raw_is_column:
                        item = PyObject_GetItem(value, select_raw.key)
                        result_value[j] = <PyObject*>item
                    elif raw_is_function_metadata:
                        item = PyObject_GetItem(value, select_raw.local_name)
                        result_value[j] = <PyObject*>item
                if num_selects == 1:
                    result_all[i] = <object>result_value[0]
                else:
                    result_all[i] = tuple([<object>result_value[j] for j in range(num_selects)])
            finally:
                free(result_value)
    finally:
        for j in range(num_selects):
            if select_types[j][0]:  # if raw_is_table
                for k in range(num_fields):
                    free(fields[j][k].name)
                free(fields[j])
        free(fields)

    return result_all

def optimize_exec_casting(values: List[Any], select_raw: List[Any], select_types: List[Tuple[bool, bool, bool]]) -> List[Any]:
    return optimize_casting(values, select_raw, select_types)
