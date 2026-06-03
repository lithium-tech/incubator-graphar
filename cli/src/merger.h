#pragma once

#include <string>
#include <pybind11/pybind11.h>

namespace py = pybind11;

std::string DoMerge(const py::dict& config_dict);
