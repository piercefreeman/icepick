from typing import Any, Type
from iceaxe.base import TableBase
from iceaxe.queries import FunctionMetadata
from json import loads as json_loads
from cpython.ref cimport PyObject
from cpython.object cimport PyObject_GetItem
from libc.stdlib cimport malloc, free

cdef list optimize_casting(list values, list select_raws, list select_types):
    cdef:
        Py_ssize_t i, j, num_values, num_selects
        list result_all
        PyObject **result_value
        object value, obj, item
        tuple select_type
        bint raw_is_table, raw_is_column, raw_is_function_metadata

    num_values = len(values)
    num_selects = len(select_raws)

    result_all = [None] * num_values

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
                    obj = select_raw(**{
                        field: (
                            value[field]
                            if not info.is_json
                            else json_loads(value[field])
                        )
                        for field, info in select_raw.model_fields.items()
                        if not info.exclude
                    })
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

    return result_all

def optimize_exec_casting(values: list[Any], select_raw: list[Any], select_types: list[tuple[bool, bool, bool]]) -> list[Any]:
    return optimize_casting(values, select_raw, select_types)
