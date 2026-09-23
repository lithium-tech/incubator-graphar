import pytest
import os
import logging
from wurlitzer import pipes

from graphar_cli._core import do_import, do_merge

from graphar_cli.config import ImportConfig, MergeConfig
from graphar_cli.settings_parser import SettingsSchema


def test_thread_num(load_settings_yaml, session_tmp_dir, settings_content_numeric):
    import_schema = load_settings_yaml("cli/test/test_data/schemas/base/import_cut.yaml.tmpl")
    merge_schema = load_settings_yaml("cli/test/test_data/schemas/base/merge.yaml.tmpl")

    import_config = ImportConfig(**import_schema, debug_mode=False)
    settings_config = SettingsSchema(**settings_content_numeric)

    with pipes() as (stdout, stderr):
        do_import(import_config.model_dump(), settings_config.model_dump())
    output = stdout.read() + stderr.read()

    assert "Using 3 threads." in output
    assert "Multi-thread mapping: starts in 3 threads" in output

    merge_config = MergeConfig(**merge_schema, debug_mode=False)
    with pipes() as (stdout, stderr):
        do_merge(merge_config.model_dump(), settings_config.model_dump())
    output = stdout.read() + stderr.read()

    assert "Building edges in 1 threads." in output
    assert "Mapping edge row to its chunk in 5 threads." in output
    assert "Using 5 threads for vertices." in output
    
    
