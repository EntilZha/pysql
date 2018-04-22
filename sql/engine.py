import sqlite3
from typing import List, Dict
import json


PYTHON_TO_SQL_TYPES = {
    type(None): 'NULL',
    int: 'INTEGER',
    float: 'REAL',
    str: 'TEXT',
    bytes: 'BLOB',
    bool: 'BOOLEAN'
}


def infer_columns(rows: List[Dict]):
    columns = {}
    for r in rows:
        for field, value in r.items():
            if field in columns:
                col_type, col_count, nullable = columns[field]
                if value is not None and col_type != type(value):
                    raise ValueError('Multiple types for one column is not allowed in SQL')
                columns[field] = (col_type, col_count + 1, nullable or value is None)
            else:
                nullable = value is None
                columns[field] = (type(value), 1, nullable)
    n_rows = len(rows)
    for col_name in columns:
        col_type, col_count, nullable = columns[col_name]
        if col_count < n_rows:
            columns[col_name] = (col_type, col_count, True)

    return [(name, PYTHON_TO_SQL_TYPES[col_type]) for name, (col_type, _, nullable) in columns.items()]


def _parse_row_field(col_name, row):
    if col_name in row:
        return row[col_name]
    else:
        return None


class SQLContext:
    def __init__(self):
        self.conn = sqlite3.connect(':memory:')

    def table_from_dicts(self, table_name, rows):
        columns = infer_columns(rows)
        col_spec = [f'{col_name} {col_type}' for col_name, col_type in columns]
        c = self.conn.cursor()
        c.execute(f'DROP TABLE IF EXISTS {table_name}')
        c.execute(f'CREATE TABLE {table_name} ({",".join(col_spec)})')
        final_rows = []
        for r in rows:
            row_values = tuple(_parse_row_field(col_name, r) for col_name, _ in columns)
            final_rows.append(row_values)
        c.executemany(f'INSERT INTO {table_name} VALUES ({",".join("?" for _ in columns)})', final_rows)
        self.conn.commit()

    def __call__(self, query):
        c = self.conn.cursor()
        c.execute(query)
        return c.fetchall()
