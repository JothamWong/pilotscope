from dataclasses import dataclass
from typing import Dict, List, Optional, Union
import psycopg2
from collections import defaultdict
import json
import argparse
from decimal import Decimal

@dataclass
class Range:
    low: Optional[Union[int, str]]
    high: Optional[Union[int, str]]
    
    def to_dict(self):
        return {
            "low": float(self.low) if isinstance(self.low, Decimal) else self.low,
            "high": float(self.high) if isinstance(self.high, Decimal) else self.high
        }
        
    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            low=data.get('low'),
            high=data.get('high')
        )

class TableStatistics:
    def __init__(self):
        self.columns: Dict[str, Union[Range, List]] = {}
        
    def to_dict(self):
        return {
            column: (value.to_dict() if isinstance(value, Range) else value)
            for column, value in self.columns.items()
        }
        
    def __repr__(self) -> str:
        return str(self.to_dict())
        
         
    @classmethod
    def from_dict(cls, data: dict):
        stats = cls()
        for column, value in data.items():
            if isinstance(value, dict) and 'low' in value and 'high' in value:
                stats.columns[column] = Range.from_dict(value)
            elif isinstance(value, str) and value.startswith('[') and value.endswith(']'):
                # Handle string representation of lists
                try:
                    # Safely evaluate string representation of list
                    import ast
                    stats.columns[column] = ast.literal_eval(value)
                except (ValueError, SyntaxError):
                    stats.columns[column] = value
            else:
                stats.columns[column] = value
        return stats
        
class StatisticsEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Range):
            return obj.to_dict()
        if isinstance(obj, TableStatistics):
            return obj.to_dict()
        return super().default(obj)

def get_db_statistics(db="jotham") -> Dict[str, TableStatistics]:
    """Gather statistics for all tables in the database."""
    statistics = defaultdict(TableStatistics)
    conn = psycopg2.connect(database=db, port="5432")
    cursor = conn.cursor()

    # Get all tables
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    tables = [row[0] for row in cursor.fetchall()]

    for table in tables:
        # Get all columns for the current table
        cursor.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{table}'
        """)
        columns = cursor.fetchall()

        for column, data_type in columns:
            print(f"At column {column}")
            # For numeric columns, get min/max
            if data_type in ('integer', 'numeric', 'bigint'):
                cursor.execute(f"""
                    SELECT MIN({column}), MAX({column})
                    FROM {table}
                    WHERE {column} IS NOT NULL
                """)
                min_val, max_val = cursor.fetchone()
                statistics[table].columns[column] = Range(min_val, max_val)

            # For character varying columns, get distinct values
            elif data_type in ('character varying', 'varchar', 'text', 'char'):
                cursor.execute(f"""
                    SELECT DISTINCT {column}
                    FROM {table}
                    WHERE {column} IS NOT NULL
                    LIMIT 1000  -- Limiting to prevent memory issues with very large sets
                """)
                distinct_values = [row[0] for row in cursor.fetchall()]
                statistics[table].columns[column] = distinct_values

            # For boolean columns, get distinct values
            elif data_type == 'boolean':
                cursor.execute(f"""
                    SELECT DISTINCT {column}
                    FROM {table}
                    WHERE {column} IS NOT NULL
                """)
                distinct_values = [row[0] for row in cursor.fetchall()]
                statistics[table].columns[column] = distinct_values

    # Example usage to print the statistics
    for table_name, table_stats in statistics.items():
        print(f"\n=== Table: {table_name} ===")
        for column_name, stats in table_stats.columns.items():
            if isinstance(stats, Range):
                print(f"{column_name}: Range(low={stats.low}, high={stats.high})")
            else:
                unique_count = len(stats)
                preview = str(stats[:5]) if stats else "[]"
                print(f"{column_name}: {unique_count} unique values, preview: {preview}")

    with open("dataset_statistics.json", 'w') as f:
        json.dump(statistics, f, indent=4, cls=StatisticsEncoder)

    return dict(statistics)

def load_statistics(filepath: str = "dataset_statistics.json") -> Dict[str, TableStatistics]:
    """
    Load statistics from a JSON file and convert back to TableStatistics objects.
    
    Args:
        filepath: Path to the JSON file containing the statistics
        
    Returns:
        Dictionary mapping table names to TableStatistics objects
    """
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            
        return {
            table_name: TableStatistics.from_dict(table_stats)
            for table_name, table_stats in data.items()
        }
    except FileNotFoundError:
        raise FileNotFoundError(f"Statistics file not found at {filepath}")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON format in file {filepath}")
    

# with open("imdb/job_train_ascii.txt", "r") as inf:
#     for i, line in enumerate(inf.readlines()):
#         with open(f"sampleout{i}.json", "w") as outf:
#             print(line)
#             json_sql = parse(line)
#             json.dump(json_sql, outf, indent=4)
#         if i > 10:
#             break


def get_dataset_sql_files(ds=str, mode=str):
    if mode not in ["train", "test"]:
        raise ValueError("Unsupported mode")        
    if ds == "imdb":
        if mode == "train":
            return f"Imbdb/job_train_ascii.txt"
        else:
            return f"Imbdb/job_test.txt"
    elif ds == "stats":
        return f"Stats/stats_{mode}.txt"
    elif ds == "tpcds":
        return f"Tpcds/tpcds_{mode}_sql.txt"
    else:
        raise ValueError("Unsupported dataset")        

        
if __name__ == "__main__":
    opts = argparse.ArgumentParser("Get dataset statistics")
    opts.add_argument("--m", type=str, choices=["g", "s"], default="g", help="g==get, s==serialize")
    args = opts.parse_args()
    
    if args.m == "g":
        get_db_statistics()
    elif args.m == "s":
        d = load_statistics()
        print(d)