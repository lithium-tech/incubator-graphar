#pragma once

#include <iostream>
#include <pybind11/pybind11.h>
#include "pybind11/stl.h"

namespace py = pybind11;

std::string DoMerge(const py::dict& config_dict)
{
    logger("Mege started");

    // getting config data
    MergeConfig merge_config;
    merge_config.fill(config_dict);

    return "Merged successfully!";
}

