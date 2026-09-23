# Contains functions to parse and interpret in-depth config
import os
import re
import math
import logging
from pathlib import Path
from typing import Any, Annotated

import yaml
import typer
from pydantic import BaseModel, ConfigDict, Field, BeforeValidator, field_validator, model_serializer
from .logging import setup_logging


setup_logging()
logger = logging.getLogger(__name__)


_THREAD_PATTERN = re.compile(
    r"^max(?:\s*(?P<op>[+\-*/])\s*(?P<num>[0-9]*\.?[0-9]+))?$"
)


_OPS = {
    "+": lambda a, b: a + b,
    "-": lambda a, b: a - b,
    "*": lambda a, b: a * b,
    "/": lambda a, b: a / b,
}


AUTO_ANSWER = -1
cpu = os.cpu_count() or 2  # assume we will have at least 2 available in any case


def parse_threads(value: Any, *, min_threads: int = 1) -> int:
    """
    Parse thread num in the settings config format.

    Returns:
        -1   for "auto"   (code will automatically plan the process)
        N>0  for other cases

    Allowed formats:
        8           → 8
        "auto"      → -1
        "max"       → os.cpu_count()
        "max/2"     → os.cpu_count() / 2
        "max - 2"   → os.cpu_count() - 2
        "max*0.75"  → int(os.cpu_count() * 0.75)
    """

    # 1. number 
    if isinstance(value, (int, float)):
        n = int(value)
        if n < 1:
            raise ValueError(f"Numer of threads must be ≥ 1, but got: {n}")
        return n

    text = str(value).strip().lower()

    # 2. auto
    if text == "auto":
        return AUTO_ANSWER

    # 3. 'max' or 'max<op>N'
    match = _THREAD_PATTERN.match(text)
    if not match:
        raise ValueError(
            f"Invalid format: '{value}'. "
            f"Allowed: number | 'auto' | 'max' | 'max/N' | 'max-N' | 'max*N' | 'max+N'"
        )

    op = match.group("op")

    # 'max' without modifications
    if op is None:
        return cpu

    operand = float(match.group("num"))

    if op == "/" and operand == 0:
        raise ValueError(f"Division by zero: {text}")

    result = int(math.floor(_OPS[op](cpu, operand)))
    return max(min_threads, min(result, os.cpu_count()))


ThreadCount = Annotated[
    int,
    BeforeValidator(parse_threads),
    Field(ge=-1, description="Threads: int>0 | 'auto'(-1) | 'max'"),
]

class ImportSettingsSchema(BaseModel):
    model_config = ConfigDict(validate_default=True)

    # Used for: reading, edge to chunk mapping, edge chunk building
    num_threads: ThreadCount = Field(default="max/2")

    @model_serializer(mode="wrap")
    def serialize_model(self, handler):
        """Custom serializer to replace -1 with default value in output."""
        data = handler(self)
        if data.get("num_threads") == AUTO_ANSWER:
            data["num_threads"] = cpu // 2
        return data


class MergeSettingsSchema(BaseModel):
    model_config = ConfigDict(validate_default=True)

    # Default: ""
    # Used for: pouring temporary mapping files on disk
    tmp_path: str = ""

    # Default: 2 (absent)
    # Used for: configuring number of memory dumps on 'tmp_path'
    num_of_drops: int = 2

    # Default: 5
    # Used for: reserving 'edge_to_chunk_mapping[thread][chunk]' using formula: 'edge_chunk_size * scale / num_threads'
    scale: int = 5

    # Used for: reading, edge to chunk mapping
    num_threads_general: ThreadCount = Field(default="max/2")

    # Used for: edge chunk building
    num_threads_build_edges: ThreadCount = Field(default="max/4")


    @field_validator("num_of_drops")
    @classmethod
    def validate_num_of_drops(cls, v: int) -> int:
        if v > 100:
            logging.warning(f"num_of_drops={v} might be too much and slow down the process.")
        if v < 2:
            msg = f"num_of_drops={v} should be absent (if so, 2 is saved) or > 1.)"
            raise ValueError(msg)
        return v

    @field_validator("scale")
    @classmethod
    def validate_scale(cls, v: int) -> int:
        if v > 50:
            logging.warning(f"[WARNING] scale={v} might be too much and slow down the process.")
        if v < 1:
            raise ValueError("scale must be >= 1.")
        return v

    @field_validator("tmp_path")
    @classmethod
    def validate_tmp_path(cls, v: str) -> str:
        if v == "auto":
            return ""
        return v


    @model_serializer(mode="wrap")
    def serialize_model(self, handler):
        """Custom serializer to replace -1 with default value in output."""
        data = handler(self)
        if data.get("num_threads_general") == AUTO_ANSWER:
            data["num_threads_general"] = cpu // 2
        if data.get("num_threads_build_edges") == AUTO_ANSWER:
            data["num_threads_build_edges"] = cpu // 4
        return data


class SettingsSchema(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    import_settings: ImportSettingsSchema = Field(alias='import')
    merge: MergeSettingsSchema


def parse_settings(path: str):

    if not Path(path).is_file():
        logger.error("File not found: %s", path)
        raise typer.Exit(1)

    with Path(path).open(encoding="utf-8") as file:
        config = yaml.safe_load(file)
    settings_config = SettingsSchema(**config)

    return settings_config
