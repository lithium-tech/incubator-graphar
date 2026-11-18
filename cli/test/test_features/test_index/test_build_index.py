import random
from collections import Counter, defaultdict
from pathlib import Path

import polars as pl
import pytest

from graphar_cli.add_index import build_attr_index

ID = "_graphArVertexIndex"


# ========== Helpers ==========
def write_chunks(dir_: Path, frames: list[pl.DataFrame]) -> list[Path]:
    dir_.mkdir(parents=True, exist_ok=True)
    paths = []
    for i, df in enumerate(frames):
        p = dir_ / f"chunk{i}"
        df.write_parquet(p, compression="snappy")
        paths.append(p)
    return paths


def reference_index(frames: list[pl.DataFrame], attr: str, id_col: str = ID) -> dict:
    groups = defaultdict(list)
    for df in frames:
        for a, i in zip(df[attr].to_list(), df[id_col].to_list()):
            groups[a].append(i)
    return {k: sorted(v) for k, v in groups.items()}


def all_input_ids(frames: list[pl.DataFrame], id_col: str = ID) -> list[int]:
    return [i for df in frames for i in df[id_col].to_list()]


def all_output_ids(result: pl.DataFrame) -> list[int]:
    return [i for ids in result[ID].to_list() if ids for i in ids]


# ========== Fixtures ==========
@pytest.fixture
def frames() -> list[pl.DataFrame]:
    return [
        pl.DataFrame({ID: [3, 0, 2, 1],    "age": [40, 30, 30, 25], "name": ["d", "a", "c", "b"]}),
        pl.DataFrame({ID: [6, 4, 7, 5],    "age": [18, 25, 40, 30], "name": ["g", "e", "h", "f"]}),
        pl.DataFrame({ID: [11, 9, 8, 10],  "age": [30, 99, 18, 25], "name": ["l", "j", "i", "k"]}),
    ]


@pytest.fixture
def chunk_paths(tmp_path: Path, frames) -> list[Path]:
    return write_chunks(tmp_path / "vertex" / "person" / "age", frames)


@pytest.fixture
def result(chunk_paths) -> pl.DataFrame:
    return build_attr_index(chunk_paths, "age")


# ========== Sort by attribute ==========
def test_sorted_by_attribute(result):
    attrs = result["age"].to_list()
    assert attrs == sorted(attrs)


def test_attribute_strictly_increasing(result):
    attrs = result["age"].to_list()
    assert all(a < b for a, b in zip(attrs, attrs[1:]))


# ========== Group by ==========
def test_each_attribute_appears_once(result):
    assert result["age"].n_unique() == result.height


def test_grouping_matches_reference(result, frames):
    expected = reference_index(frames, "age")
    actual = {a: ids for a, ids in result.iter_rows()}
    assert actual == expected


def test_group_not_split_across_chunks(result):
    row = result.filter(pl.col("age") == 30)
    assert row.height == 1
    assert row[ID][0].to_list() == [0, 2, 5, 11]


def test_ids_sorted_within_group(result):
    for ids in result[ID].to_list():
        assert ids == sorted(ids)


def test_single_id_is_still_list(result):
    row = result.filter(pl.col("age") == 99)
    assert row[ID][0].to_list() == [9]


# ========== Data is same ==========
def test_no_ids_lost_or_added(result, frames):
    assert Counter(all_output_ids(result)) == Counter(all_input_ids(frames))


def test_total_count_preserved(result, frames):
    total_in = sum(df.height for df in frames)
    assert result[ID].list.len().sum() == total_in


def test_attribute_values_preserved(result, frames):
    attrs_in = {a for df in frames for a in df["age"].to_list()}
    assert set(result["age"].to_list()) == attrs_in


def test_each_id_in_exactly_one_group(result):
    ids = all_output_ids(result)
    assert len(ids) == len(set(ids))


# ========== Additional tests ==========
def test_output_schema(result):
    assert result.columns == ["age", ID]
    assert result.schema["age"] == pl.Int64
    assert result.schema[ID] == pl.List(pl.Int64)


def test_chunk_order_does_not_matter(chunk_paths):
    straight = build_attr_index(chunk_paths, "age")
    reversed_ = build_attr_index(list(reversed(chunk_paths)), "age")
    assert straight.equals(reversed_)


def test_deterministic(chunk_paths):
    first = build_attr_index(chunk_paths, "age")
    for _ in range(5):
        assert build_attr_index(chunk_paths, "age").equals(first)


def test_no_null_ids_in_output(result):
    s = result[ID]
    assert s.null_count() == 0, "null instead of id list"
    assert (
        s.list.len() == s.list.drop_nulls().list.len()
    ).all(), "null inside id lists"


def test_accepts_generator(chunk_paths):
    result = build_attr_index((p for p in chunk_paths), "age")
    assert result.height > 0


def test_string_attribute(tmp_path):
    frames = [
        pl.DataFrame({ID: [0, 1, 2], "name": ["bob", "alice", "bob"]}),
        pl.DataFrame({ID: [3, 4, 5], "name": ["Alice", "carol", "alice"]}),
    ]
    paths = write_chunks(tmp_path / "name", frames)
    result = build_attr_index(paths, "name")

    assert result["name"].to_list() == ["Alice", "alice", "bob", "carol"]
    assert {a: ids for a, ids in result.iter_rows()} == reference_index(frames, "name")


def test_null_attribute_values_not_lost(tmp_path):
    frames = [
        pl.DataFrame({ID: [0, 1, 2], "age": [30, None, 25]}),
        pl.DataFrame({ID: [3, 4, 5], "age": [None, 25, None]}),
    ]
    paths = write_chunks(tmp_path / "age", frames)
    result = build_attr_index(paths, "age")

    null_rows = result.filter(pl.col("age").is_null())
    assert null_rows.height == 1
    assert null_rows[ID][0].to_list() == [1, 3, 5]

    non_null = result.filter(pl.col("age").is_not_null())["age"].to_list()
    assert non_null == sorted(non_null)

    assert Counter(all_output_ids(result)) == Counter(all_input_ids(frames))


def test_empty_chunk_among_others(tmp_path):
    empty = pl.DataFrame({ID: pl.Series([], dtype=pl.Int64), "age": pl.Series([], dtype=pl.Int64)})
    frames = [
        pl.DataFrame({ID: [0, 1], "age": [5, 3]}),
        empty,
        pl.DataFrame({ID: [2, 3], "age": [3, 7]}),
    ]
    paths = write_chunks(tmp_path / "age", frames)
    result = build_attr_index(paths, "age")
    assert {a: ids for a, ids in result.iter_rows()} == reference_index(frames, "age")


def test_custom_id_column(tmp_path):
    frames = [pl.DataFrame({"vid": [2, 0, 1], "age": [1, 1, 2]})]
    paths = write_chunks(tmp_path / "age", frames)
    result = build_attr_index(paths, "age", id_column="vid")

    assert result.columns == ["age", ID]
    assert {a: ids for a, ids in result.iter_rows()} == reference_index(frames, "age", "vid")


def test_empty_paths_raises():
    with pytest.raises(FileNotFoundError):
        build_attr_index([], "age")


def test_missing_attribute_column_raises(chunk_paths):
    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        build_attr_index(chunk_paths, "no_such_column")


def test_missing_id_column_raises(tmp_path):
    paths = write_chunks(tmp_path / "age", [pl.DataFrame({"age": [1, 2]})])
    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        build_attr_index(paths, "age")


@pytest.mark.parametrize("seed", range(5))
def test_random_data_matches_reference(tmp_path, seed):
    rng = random.Random(seed)
    n_vertices, chunk_size, n_distinct = 1000, 64, 50

    ids = list(range(n_vertices))
    frames = []
    for start in range(0, n_vertices, chunk_size):
        chunk_ids = ids[start:start + chunk_size]
        rng.shuffle(chunk_ids)
        frames.append(pl.DataFrame({
            ID: chunk_ids,
            "score": [rng.randrange(n_distinct) for _ in chunk_ids],
        }))

    paths = write_chunks(tmp_path / f"score_{seed}", frames)
    result = build_attr_index(paths, "score")

    expected = reference_index(frames, "score")
    assert result.rows() == sorted(expected.items())