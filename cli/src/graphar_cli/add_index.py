from typing import Any, Iterable
from pathlib import Path
import polars as pl
import yaml


# ========== Build index ==========
def build_attr_index(
    chunk_paths: Iterable[Path],
    attr_column: str,
    id_column: str = "_graphArVertexIndex",
) -> pl.DataFrame:
    paths = [str(p) for p in chunk_paths]
    if not paths:
        raise FileNotFoundError("No chunk files found")

    return (
        pl.scan_parquet(paths)
        .select([attr_column, id_column])
        .group_by(attr_column)
        .agg(pl.col(id_column).sort().alias("_graphArVertexIndex"))
        .sort(attr_column)
        .collect()
    )


# ========== Helpers ==========
def find_chunks(property_dir: Path) -> list[Path]:
    chunks = [
        p for p in property_dir.glob("chunk*")
        if p.is_file() and p.name.removeprefix("chunk").isdigit()
    ]
    return sorted(chunks, key=lambda p: int(p.name.removeprefix("chunk")))


def write_chunks(df: pl.DataFrame, output_dir: Path, chunk_size: int) -> int:
    if chunk_size <= 0:
        raise ValueError(f"chunk_size must be positive, got {chunk_size}")

    output_dir.mkdir(parents=True, exist_ok=True)

    for old in find_chunks(output_dir):
        old.unlink()

    n_chunks = 0
    for i, part in enumerate(df.iter_slices(n_rows=chunk_size)):
        part.write_parquet(output_dir / f"chunk{i}", compression="snappy")
        n_chunks += 1
    return n_chunks


def link_type_to_metadata(graph_description: Path) -> dict:

    obj_types = ["vertices"]  # TODO: "edges"
    type_to_metadata = {t: dict() for t in obj_types}

    graph_root = graph_description.parent

    with open(graph_description, "r", encoding="utf-8") as f:
        graph_info = yaml.safe_load(f)

    for obj_type in obj_types:
        for v_path in graph_info[obj_type]:

            with open(graph_root / v_path, "r", encoding="utf-8") as f:
                vertex_info = yaml.safe_load(f)

            type_to_metadata[obj_type][vertex_info["type"]] = graph_root / v_path

    return type_to_metadata


# ========== Main function ==========
def run_add_index(config: dict[str, Any]) -> str:
    graph_root = Path(config["graphar"]["path"])

    type_to_metadata = link_type_to_metadata(graph_root / f"{config['graphar']['name']}.yaml")

    for vertex in config["vertices"]:
        vtype = vertex["type"]
        print(f"Building index for vertex '{vtype}'.")

        vertex_path = type_to_metadata["vertices"][vtype]
        if vertex_path is None:
            print(f"[ERROR] No mmetadata for vetrex '{vtype}' found.")
            continue
        print(f"\tReading vertex info from '{vertex_path}'.")

        with open(vertex_path, "r", encoding="utf-8") as f:
            vertex_info = yaml.safe_load(f)
        vertex_data_dir = graph_root / vertex_info["prefix"]

        for attr in vertex["properties"]:
            print(f"\tBuilding index for '{attr}'.")

            property_dir = None
            for pg in vertex_info["property_groups"]:
                if any(prop["name"] == attr for prop in pg["properties"]):
                    property_dir = vertex_data_dir / pg["prefix"]
                    break
            if property_dir is None:
                print(f"    Could not find property '{attr}' in vertex '{vtype}'.")
                continue

            chunks = find_chunks(property_dir)
            index_df = build_attr_index(chunks, attr)

            output_dir = graph_root / "index" / "vertex" / vtype / attr

            if vertex["chunk_size"]:
                chunk_size = vertex["chunk_size"]
            else:
                chunk_size = vertex_info["chunk_size"]   
            print(f"\t\tVertex chunk size: {chunk_size}")

            n = write_chunks(index_df, output_dir, chunk_size)
            print(f"\t\tWrote {index_df.height} rows in {n} chunk(s) to {output_dir}")

    return "Successfully added index."