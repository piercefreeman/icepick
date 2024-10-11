use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

#[pymodule]
fn iceaxe(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    #[pyfn(m)]
    #[pyo3(name = "exec")]
    fn exec(
        py: Python<'_>,
        select_raw: Vec<Py<PyAny>>,
        select_types: Vec<(bool, bool, bool)>,
        values: Vec<&PyDict>,
    ) -> PyResult<Vec<PyObject>> {
        let mut result_all = Vec::new();
        /*let select_types: Vec<_> = select_raw
        .iter()
        .map(|obj| -> PyResult<_> {
            Ok((
                obj.bind(py).getattr("is_base_table")?.extract::<bool>()?,
                obj.bind(py).getattr("is_column")?.extract::<bool>()?,
                obj.bind(py)
                    .getattr("is_function_metadata")?
                    .extract::<bool>()?,
            ))
        })
        .collect::<PyResult<_>>()?;*/

        for value in values {
            let mut result_value = Vec::new();
            for (select_obj, (is_table, is_column, is_function_metadata)) in
                select_raw.iter().zip(select_types.iter())
            {
                if *is_table {
                    let model_fields = select_obj.bind(py).getattr("model_fields")?;
                    let kwargs = PyDict::new_bound(py);
                    for result in model_fields.iter()? {
                        let item = result?;
                        let field: String = item.get_item(0)?.extract()?;
                        let info = item.get_item(1)?;
                        if !info.getattr("exclude")?.extract::<bool>()? {
                            match value.get_item(&field) {
                                Ok(Some(field_value)) => {
                                    if info.getattr("is_json")?.extract::<bool>()? {
                                        let json_str: String = field_value.extract()?;
                                        let parsed_json: Py<PyAny> = py
                                            .import_bound("json")?
                                            .call_method1("loads", (json_str,))?
                                            .into();
                                        kwargs.set_item(&field, parsed_json)?;
                                    } else {
                                        kwargs.set_item(&field, field_value)?;
                                    }
                                }
                                Ok(None) => {
                                    println!("Field {} not found in value", field);
                                }
                                Err(e) => {
                                    println!("Error getting field {}: {:?}", field, e);
                                }
                            }
                        }
                    }
                    let instance = select_obj.bind(py).call(((), kwargs), None)?;
                    result_value.push(instance.into_py(py));
                } else if *is_column {
                    let key = select_obj.bind(py).getattr("key")?;
                    match value.get_item(key) {
                        Ok(Some(field_value)) => {
                            result_value.push(field_value.into_py(py));
                        }
                        Ok(None) => {
                            println!("Column key not found in value");
                        }
                        Err(e) => {
                            println!("Error getting column key: {:?}", e);
                        }
                    }
                } else if *is_function_metadata {
                    let local_name = select_obj.bind(py).getattr("local_name")?;
                    match value.get_item(local_name) {
                        Ok(Some(field_value)) => {
                            result_value.push(field_value.into_py(py));
                        }
                        Ok(None) => {
                            println!("Function metadata local name not found in value");
                        }
                        Err(e) => {
                            println!("Error getting function metadata local name: {:?}", e);
                        }
                    }
                }
            }
            let result = if result_value.len() == 1 {
                result_value.pop().unwrap()
            } else {
                PyTuple::new_bound(py, result_value).into()
            };
            result_all.push(result);
        }
        Ok(result_all)
    }

    Ok(())
}
