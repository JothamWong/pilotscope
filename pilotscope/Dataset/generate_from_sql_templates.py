from mo_sql_parsing import parse, format
import json
from typing import Dict, Any, Union, List, Set, Tuple
import argparse
from collections import defaultdict
import os
import random
import traceback

def parameterize_concrete_sql(sql_template: Dict, tables=None) -> Dict:
    """Convert a templated SQL AST to a concrete SQL query following the 
    provided distribution"""
    def process_value(value: Any) -> Any:
        if isinstance(value, dict):
            return parameterize_concrete_sql(value, tables)
        elif isinstance(value, list):
            return [process_value(item) for item in value]
        return value
    
    def is_column(param: Union[str, Dict]):
        return isinstance(param, str)
    
    # One time operation to get aliases
    result = {}
    if tables is None:
        tables = {}
        table_aliases = sql_template["from"]
        for alias in table_aliases:
            og_table = alias["value"]
            table_name = alias["name"]
            tables[table_name] = og_table
    for key, value in sql_template.items():
        if key in ["eq", "gt", "lt", "gte", "lte"]:
            left, right = value[0], value[1]
            # Non-templated value, nothing to do here
            if is_column(left) and is_column(right):
                result[key] = value
                continue
            if is_column(left):
                ref = value[0]
                placeholder = value[1]["literal"]
            else:
                ref = value[1]
                placeholder = value[0]["literal"]
            # Form is table_alias.table_col
            table_ref_alias, col_ref = ref.split(".")
            table_ref = tables[table_ref_alias]
            # TODO: Place based off distribution passed off table
            # At the moment, use a random
            if placeholder == "NUMBER":
                ranges = statistics[table_ref][col_ref]
                result[key] = [ref, random.randint(ranges["low"], ranges["high"])]
            elif placeholder == "STRING":
                result[key] = [ref, {"literal":random.choice(statistics[table_ref][col_ref])}]
            else:
                raise RuntimeError("Illegal placeholder type")
        elif key == "between":
            ref = value[0]
            table_ref_alias, col_ref = ref.split(".")
            table_ref = tables[table_ref_alias]
            ranges = statistics[table_ref][col_ref]
            print(key)
            print(ref)
            col_type = value[1]
            print(ranges)
            if col_type == "STRING":
                v1, v2 = random.sample(ranges, 2)
            elif col_type == "NUMBER":
                v1 = random.randint(ranges["low"], ranges["high"])
                v2 = random.randint(ranges["low"], ranges["high"])
            if v1 < v2:
                result[key] = [ref, v1, v2]
            else:
                result[key] = [ref, v2, v1]
        elif key == "in":
            ref = value[0]
            table_ref_alias, col_ref = ref.split(".")
            table_ref = tables[table_ref_alias]
            possible_values = statistics[table_ref][col_ref]
            num_to_gen = random.randint(1, len(possible_values))
            values = random.sample(possible_values, num_to_gen)
            result[key] = [ref, {"literal": values}]
        elif isinstance(value, dict):
            result[key] = parameterize_concrete_sql(value, tables)
        elif isinstance(value, list):
            result[key] = [process_value(item) for item in value]
        else:
            result[key] = value
    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Extract SQL query templates")
    parser.add_argument("--input-dir", type=str, required=True, help="Input SQL directory")
    parser.add_argument("--stats-file", type=str, required=True, help="SQL benchmark statistics file")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="sql_new_files",
        help="Output directory for new sql files",
    )
    args = parser.parse_args()
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)
    
    with open(args.stats_file, "r") as inf:
        statistics = json.loads(inf.read())
    
    for root, dirs, files in os.walk(args.input_dir):
        for file in files:
            pathname = os.path.join(root, file)
            print(f"Handling {pathname}")
            with open(pathname, "r") as sqlf:
                sql = sqlf.read()
                sql_ast = parse(sql)
                try:
                    parameterized_sql_ast = parameterize_concrete_sql(sql_ast)
                except Exception as e:
                    tb = traceback.print_exc()
                    print(tb)
                sql_text = format(parameterized_sql_ast)
                output_pathname = os.path.join(output_dir, file)
                with open(output_pathname, "w") as outf:
                    outf.write(sql_text)