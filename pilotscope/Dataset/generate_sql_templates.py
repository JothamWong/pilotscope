from mo_sql_parsing import parse, format
import json
from typing import Dict, Any, Union, List, Set, Tuple
import argparse
from collections import defaultdict
import traceback


def convert_to_template(parsed_sql: Dict, tables=None) -> Dict:
    """Convert parsed SQL AST to a template by replacing literal values with placeholders."""
    def process_value(value: Any) -> Any:
        if isinstance(value, dict):
            return convert_to_template(value, tables=tables)
        elif isinstance(value, list):
            return [process_value(item) for item in value]
        return value

    def is_column(param: Union[str, Dict]):
        return isinstance(param, str)

    result = {}
    # First get all the sets and their aliases, one time operation
    if tables is None:
        tables = set()
        for table in parsed_sql["from"]:
            tables.add(table["name"])

    for key, value in parsed_sql.items():
        if key in ["eq", "gt", "lt", "gte", "lte", "between"]:
            if isinstance(value, list) and len(value) == 2:
                left = value[0]
                right = value[1]
                # Both are referencing columns directly, nothing to do here
                if is_column(left) and is_column(right):
                    result[key] = value
                    continue
                # At least one is a variable
                if is_column(left):
                    col_ref = left
                    current_val = right
                else:
                    col_ref = right
                    current_val = left

                if isinstance(current_val, (int, float)):
                    result[key] = {col_ref: "NUMBER"}
                elif isinstance(current_val, dict):
                    result[key] = {col_ref: "STRING"}
                elif isinstance(current_val, str):
                    result[key] = {col_ref: "STRING"}
                elif isinstance(current_val, list):
                    result[key] = {
                        col_ref: [
                            "NUMBER" if isinstance(v, (int, float)) else "STRING"
                            for v in current_val
                        ]
                    }
                else:
                    result[key] = value
            elif isinstance(value, list) and len(value) == 3:
                # BETWEEN CLAUSES
                col_ref = value[0]
                current_val = value[1]
                if isinstance(current_val, (int, float)):
                    t = "NUMBER"
                else:
                    t = "STRING"
                result[key] = [col_ref, t, t]
        # Handle IN clauses
        elif key == "in":
            if isinstance(value, list):
                col_ref = value[0]
                result[key] = [col_ref, "LIST"]
        # Recursively process nested structures
        elif isinstance(value, dict):
            result[key] = convert_to_template(value, tables=tables)
        elif isinstance(value, list):
            result[key] = [process_value(item) for item in value]
        else:
            result[key] = value
    return result


class QueryTemplateExtractor:
    def __init__(self):
        self.templates = defaultdict(set)  # hash -> set of original queries
        self.template_to_ast = {}  # hash -> template AST

    def get_template_hash(self, template_ast: Dict) -> str:
        """Get a hash of the template AST for deduplication."""
        return json.dumps(template_ast, sort_keys=True)

    def add_query(self, query: str):
        """Parse a query and add its template to the collection."""
        try:
            parsed = parse(query.strip())
            template = convert_to_template(parsed)
            template_hash = self.get_template_hash(template)

            self.templates[template_hash].add(query.strip())
            self.template_to_ast[template_hash] = template

        except Exception as e:
            print(f"Error processing query: {query}")
            print(f"Error: {e}")
            exit()

    def save_templates(self, output_dir: str):
        """Save templates and their examples to files."""
        import os

        os.makedirs(output_dir, exist_ok=True)
        # Save template mapping
        num_errors = 0
        template_mapping = {}
        for i, (template_hash, ast) in enumerate(self.template_to_ast.items()):
            try:
                template_mapping[i] = {
                    "template": format(ast),
                    "ast": ast,
                    "examples": list(self.templates[template_hash]),
                }
            except Exception as e:
                # Really dumb stuff with movie_idx having like multiple dots in their string literals...
                num_errors += 1
                tb = traceback.format_exc()
                print(tb)
                exit()

        with open(os.path.join(output_dir, "template_mapping.json"), "w") as f:
            json.dump(template_mapping, f, indent=2)

        # Save individual template files
        template_dir = os.path.join(output_dir, "templates")
        os.makedirs(template_dir, exist_ok=True)

        for template_id, info in template_mapping.items():
            with open(
                os.path.join(template_dir, f"template_{template_id}.sql"), "w"
            ) as f:
                f.write(info["template"])

        print(f"Found {len(self.templates)} unique query templates")
        print(f"Had {num_errors} errored out query templates")
        return template_mapping


def process_sql_file(input_file: str, output_dir: str) -> Dict:
    """Process SQL queries from a file and extract templates."""
    extractor = QueryTemplateExtractor()

    with open(input_file, "r") as f:
        queries = f.readlines()
    for i, query in enumerate(queries):
        query = query.strip()
        if query:
            extractor.add_query(query)

    return extractor.save_templates(output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Extract SQL query templates")
    parser.add_argument("--input", type=str, required=True, help="Input SQL file")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="sql_templates",
        help="Output directory for templates",
    )
    args = parser.parse_args()

    template_mapping = process_sql_file(args.input, args.output_dir)
