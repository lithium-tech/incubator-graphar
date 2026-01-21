#pragma once

#include <iostream>
#include <pybind11/pybind11.h>
#include "pybind11/stl.h"

namespace py = pybind11;

std::string DoMerge(const py::dict& config_dict)
{
    std::cout << "Mege started" << std::endl;
    return "Merged successfully!";
}

