import pyarrow.parquet as pq
import yaml
from io import StringIO


from graphar_cli.checker import get_all_files


# ========== file managers ==========
def find_all_paths(filename: str) -> list[[str, str]]:
    """Find all paths to folders with properties.
    
    filename: name of main GraphAr file
    """
    # find path to graph
    path2graph = '/'.join(filename.split('/')[:-1])

    # read vertices & edges of graph
    with open(filename, "r") as f:
        graph_data = yaml.safe_load(f)
        vertices = graph_data["vertices"]
        edges = graph_data["edges"]

    # collect all tables
    all_tables = []

    # collect paths to vertex tables
    for vertex in vertices:
        path_to_vertex = path2graph + '/' + vertex
        with open(path_to_vertex, "r") as f:
            vertex_data = yaml.safe_load(f)
            prefix = vertex_data["prefix"]
            path_to_vertex = path2graph + '/' + prefix
            for property_group in vertex_data["property_groups"]:
                all_tables.append([path_to_vertex + property_group["prefix"], vertex])

    # collect paths to edge tables
    for edge in edges:
        path_to_edge = path2graph + '/' + edge
        with open(path_to_edge, "r") as f:
            edge_data = yaml.safe_load(f)
            path_to_edge = path2graph + '/' + edge_data["prefix"] + '/' + edge_data["adj_lists"][0]["prefix"]
            for property_group in edge_data["property_groups"]:
                all_tables.append([path_to_edge + property_group["prefix"], edge])

    return all_tables


def add_extra_info(file: str, extra_info: dict) -> None:

    with open(file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    if config is None:
        raise ValueError(f"Path '{file}' leads to an empty file which is not a GraphAr config.\nCan't write collected data:\n{extra_info}")

    stream = StringIO()
    yaml.dump(
        extra_info, 
        stream, 
        default_flow_style=False,  
        sort_keys=False,           
        allow_unicode=True,        
        indent=2                   
    )
    value_str = stream.getvalue()

    new_entry = {
        "key": "statistics",
        "value": value_str
    }

    if 'extra_info' not in config:
        config['extra_info'] = []
    
    stats_entry = next((item for item in config['extra_info'] if item.get("key") == "statistics"), None)
    if stats_entry:
        stats_entry["value"] = value_str
    else:
        config['extra_info'].append(new_entry)

    def str_representer(dumper, data):
        if '\n' in data:
            return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
        return dumper.represent_scalar('tag:yaml.org,2002:str', data)

    yaml.add_representer(str, str_representer, Dumper=yaml.SafeDumper)

    with open(file, 'w', encoding='utf-8') as f:
        yaml.dump(
            config, 
            f, 
            default_flow_style=False, 
            sort_keys=False, 
            allow_unicode=True, 
            indent=2,
            Dumper=yaml.SafeDumper
        )


# ========== gather min/max statistics ==========
def calculate_minmax(tables: list[str], name: str) -> dict:
    stats = {}

    pf0 = pq.ParquetFile(tables[0])
    for column in pf0.schema.names:
        if name not in stats: stats[name] = dict()
        if column not in stats[name]: stats[name][column] = dict()
        if "min" not in stats[name][column]: stats[name][column] = {"min": None, "max": None}

    for table in tables:
        pf = pq.ParquetFile(table)

        for rg in range(pf.num_row_groups):
            row_group = pf.metadata.row_group(rg)

            for col_idx, col_name in enumerate(pf.schema.names):
                col = row_group.column(col_idx)
                col_stats = col.statistics

                if col_stats is None:
                    continue

                if col_stats.has_min_max:
                    value_min = col_stats.min
                    value_max = col_stats.max
                    
                    if stats[name][col_name]["min"] is None:
                        stats[name][col_name]["min"] = value_min
                    else:
                        stats[name][col_name]["min"] = min(stats[name][col_name]["min"], value_min)

                    if stats[name][col_name]["max"] is None:
                        stats[name][col_name]["max"] = value_max
                    else:
                        stats[name][col_name]["max"] = max(stats[name][col_name]["max"], value_max)
    return stats


def calculate_statistics(config_file: str):

    stats = []
    all_tables = find_all_paths(config_file)
    for table_path, table_name in all_tables:
        table_name_short = table_name.split('.')[0]
        all_files = get_all_files(table_path)
        tmp_stats = calculate_minmax(all_files, table_name_short)
        
        # check if we already have statistics for this vertex/edge
        entry = None
        for item in stats:
            if table_name_short == list(item.keys())[0]:
                entry = item
                break

        if not entry:
            stats.append(tmp_stats)
        else:

            for k, v in tmp_stats[list(tmp_stats.keys())[0]].items():
                entry[table_name_short][k] = v

    add_extra_info(config_file, stats)

    return "Calculation complete."
