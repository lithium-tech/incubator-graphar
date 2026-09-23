import pytest
import tempfile
import yaml
from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent.parent

@pytest.fixture
def project_root():
    """Return absolute path to project root."""
    return PROJECT_ROOT


def _replace_paths(obj, replacements: dict) -> dict:
    """
    Recursively replace path placeholders in dict/list.
    
    Args:
        obj: Dict or list to process
        replacements: {placeholder: absolute_path}
    
    Returns:
        Processed dict/list with replaced paths
    """
    if isinstance(obj, dict):
        return {k: _replace_paths(v, replacements) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_replace_paths(item, replacements) for item in obj]
    elif isinstance(obj, str):
        for placeholder, path in replacements.items():
            obj = obj.replace(placeholder, str(path))
        return obj
    else:
        return obj


@pytest.fixture
def valid_settings_content():
    return {
        'import': {
            'num_threads': 'max/3',
        },
        'merge': {
            'tmp_path': '/some/tmp/path',
            'num_of_drops': 5,
            'scale': 10,
            'num_threads_general': 'auto',
            'num_threads_build_edges': 4,
        }
    }


@pytest.fixture
def settings_content_numeric():
    return {
        'import': {
            'num_threads': 3,
        },
        'merge': {
            'num_threads_general': 5,
            'num_threads_build_edges': 1,
        }
    }


@pytest.fixture(scope="session")
def session_tmp_dir(tmp_path_factory):
    """
    One temp directory for all tests in the session.
    Persists across tests, cleaned up at the end.
    """
    return tmp_path_factory.mktemp("graphar_result")


@pytest.fixture
def load_settings_yaml(project_root, session_tmp_dir):
    def _load(relative_path: str, overrides: dict = None) -> dict:
        yaml_path = PROJECT_ROOT / relative_path
        print(PROJECT_ROOT)
        
        with yaml_path.open('r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        replacements = {
            '${ROOT_DIR}': PROJECT_ROOT,
            '${GRAPHAR_RESULT}': session_tmp_dir,
        }
        config = _replace_paths(config, replacements)
        
        if overrides:
            for key, value in overrides.items():
                if key in config and isinstance(value, dict):
                    config[key].update(value)
                else:
                    config[key] = value
        
        return config
    
    return _load
