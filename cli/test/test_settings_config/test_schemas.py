import pytest
import os
import logging
import typer
from pydantic import ValidationError
import yaml
from graphar_cli.settings_parser import (
    ImportSettingsSchema,
    MergeSettingsSchema,
    SettingsSchema,
    parse_threads,
    parse_settings,
    AUTO_ANSWER,
    cpu,
)

# =============================================================================
# parse_threads tests
# =============================================================================

def test_parse_threads_integer_input():
    """Integer values pass through as-is."""
    assert parse_threads(8) == 8


def test_parse_threads_float_input():
    """Float values are rounded down."""
    assert parse_threads(8.9) == 8


def test_parse_threads_auto_keyword():
    """'auto' keyword returns AUTO_ANSWER (-1)."""
    assert parse_threads("auto") == AUTO_ANSWER
    assert parse_threads("AUTO") == AUTO_ANSWER


def test_parse_threads_max_keyword():
    """'max' returns CPU count."""
    assert parse_threads("max") == cpu


def test_parse_threads_max_expressions():
    """Expressions with max (/, *, +, -)."""
    
    assert parse_threads("max/2") == cpu // 2
    assert parse_threads("max*0.75") == int(cpu * 0.75)
    assert parse_threads("max*1.75") == cpu
    assert parse_threads("max+2") == cpu
    assert parse_threads("max - 2") == max(1, cpu - 2)
    assert parse_threads("max/0.5") == cpu

def test_parse_threads_invalid_value():
    """Invalid value raises ValueError."""
    with pytest.raises(ValueError):
        parse_threads(-1)

    with pytest.raises(ValueError):
        parse_threads(-3)

    with pytest.raises(ValueError):
        parse_threads(0)


def test_parse_threads_division_by_zero():
    """Division by zero raises ValueError."""
    with pytest.raises(ValueError, match="Division by zero"):
        parse_threads("max/0")


# =============================================================================
# ImportSettingsSchema tests
# =============================================================================

def test_import_settings_default_values():
    """Default values are correct, no -1."""
    settings = ImportSettingsSchema()
    assert isinstance(settings.num_threads, int)
    assert settings.num_threads == cpu // 2


def test_import_settings_custom_threads():
    """Custom threads can be set."""

    settings = ImportSettingsSchema(num_threads="auto")
    assert settings.num_threads == AUTO_ANSWER

    settings = ImportSettingsSchema(num_threads=3)
    assert settings.num_threads == 3

# =============================================================================
# MergeSettingsSchema tests
# =============================================================================

def test_merge_settings_valid_values():
    """Valid values are accepted."""
    settings = MergeSettingsSchema(num_of_drops=10, scale=20)
    assert settings.num_of_drops == 10
    assert settings.scale == 20


def test_merge_settings_invalid_drops_low():
    """num_of_drops < 2 raises error."""
    with pytest.raises(ValueError, match="num_of_drops"):
        MergeSettingsSchema(num_of_drops=1)

    with pytest.raises(ValueError, match="num_of_drops"):
        MergeSettingsSchema(num_of_drops=-2)
    
def test_merge_settings_invalid_scale_low():
    """num_of_drops < 1 raises error."""
    with pytest.raises(ValueError, match="scale"):
        MergeSettingsSchema(scale=-1)
    
    with pytest.raises(ValueError, match="scale"):
        MergeSettingsSchema(scale=0)

def test_merge_settings_high_drops_warning(caplog):
    """num_of_drops > 100 triggers warning."""
    caplog.set_level(logging.WARNING)
    settings = MergeSettingsSchema(num_of_drops=150)
    assert settings.num_of_drops == 150
    assert "might be too much" in caplog.text


# =============================================================================
# SettingsSchema tests
# =============================================================================

def test_settings_schema_full_valid(valid_settings_content):
    """Full valid configuration."""

    settings = SettingsSchema(**valid_settings_content)
    data = settings.model_dump()

    assert settings.import_settings.num_threads == cpu // 3
    assert settings.merge.tmp_path == "/some/tmp/path"
    assert settings.merge.num_of_drops == 5
    assert settings.merge.scale == 10
    assert settings.merge.num_threads_general == -1 
    assert settings.merge.num_threads_build_edges == 4  

    assert data['import_settings']['num_threads'] == cpu // 3
    assert data['merge']['tmp_path'] == "/some/tmp/path"
    assert data['merge']['num_of_drops'] == 5
    assert data['merge']['scale'] == 10
    assert data['merge']['num_threads_general'] == cpu // 2 
    assert data['merge']['num_threads_build_edges'] == 4
    

def test_settings_schema_empty_valid():
    settings = SettingsSchema(
        import_settings=ImportSettingsSchema(),
        merge=MergeSettingsSchema()
    )
    data = settings.model_dump()

    assert data['import_settings']['num_threads'] == cpu // 2
    assert data['merge']['tmp_path'] == ""
    assert data['merge']['num_of_drops'] == 2
    assert data['merge']['scale'] == 5
    assert data['merge']['num_threads_general'] == cpu // 2 
    assert data['merge']['num_threads_build_edges'] == cpu // 4


def test_settings_schema_auto_valid():
    # 'auto' is equal to the empty schema
    data = {
        'import': {
            'num_threads': 'auto',
        },
        'merge': {
            'tmp_path': 'auto',
            'num_threads_general': 'auto',
            'num_threads_build_edges': 'auto',
        }
    }

    settings = SettingsSchema(**data)
    data = settings.model_dump()

    assert data['import_settings']['num_threads'] == cpu // 2
    assert data['merge']['tmp_path'] == ""
    assert data['merge']['num_of_drops'] == 2
    assert data['merge']['scale'] == 5
    assert data['merge']['num_threads_general'] == cpu // 2 
    assert data['merge']['num_threads_build_edges'] == cpu // 4


def test_settings_schema_invalid_value():
    data = {
        'import': {
            'num_threads': 'invalid',
        },
        'merge': {
            'tmp_path': 's123',
            'num_threads_general': 'auto',
            'num_threads_build_edges': 'auto',
        }
    }

    with pytest.raises(ValidationError):
        SettingsSchema(**data)


# =============================================================================
# parse_settings tests
# =============================================================================

def test_parse_settings_no_file_on_disk(tmp_path):
    missing_file = tmp_path / "does_not_exist.yaml"
    with pytest.raises(typer.Exit) as exc_info:
        parse_settings(str(missing_file))
    assert exc_info.value.exit_code == 1

def test_parse_settings_invalid_yaml(tmp_path):
    bad_file = tmp_path / "bad.yaml"
    bad_file.write_text("import: [unclosed\n  bracket: {", encoding="utf-8")
    with pytest.raises(yaml.YAMLError):
        parse_settings(str(bad_file))

def test_parse_settings_valid_yaml_missing_required(tmp_path):
    """Отсутствует обязательное поле 'merge'."""
    yaml_content = """
    import:
        num_threads: 4
    """
    config_file = tmp_path / "incomplete.yaml"
    config_file.write_text(yaml_content, encoding="utf-8")

    with pytest.raises(Exception):  # pydantic.ValidationError
        parse_settings(str(config_file))


# =============================================================================
# regex tests
# =============================================================================

def test_max_with_space():
    assert parse_threads("max /2") == cpu // 2
    assert parse_threads("max/ 2") == cpu // 2
    assert parse_threads("max / 2") == cpu // 2
    assert parse_threads("  max/2  ") == cpu // 2


def test_max_uppercase():
    assert parse_threads("MAX") == cpu
    assert parse_threads("Max") == cpu

@pytest.mark.parametrize("bad_input", [
    "max/", "max//2", "max/-2", "max/two", "", "   ", "invalid" 
])
def test_parse_threads_invalid_strings(bad_input):
    with pytest.raises(ValueError):
        parse_threads(bad_input)
