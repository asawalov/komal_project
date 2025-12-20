"""
Formula Engine Module

A mini Excel calculation engine that supports:
- All Excel formulas and functions
- Cross-sheet references
- Dependency resolution with circular reference detection
- Excel-compatible error messages

Uses pandas eval for simple arithmetic and formulas library for Excel functions.
"""

import re
import pandas as pd
import numpy as np
from collections import defaultdict
from typing import Dict, List, Any, Optional, Tuple, Set
import warnings

warnings.filterwarnings('ignore')

# Common Excel functions for reference checking
EXCEL_FUNCTIONS = {
    'SUM', 'AVERAGE', 'COUNT', 'MAX', 'MIN', 'IF', 'IFS', 'AND', 'OR', 'NOT',
    'VLOOKUP', 'HLOOKUP', 'INDEX', 'MATCH', 'XLOOKUP', 'LOOKUP',
    'LEFT', 'RIGHT', 'MID', 'LEN', 'CONCAT', 'CONCATENATE', 'TEXT', 'UPPER', 'LOWER', 'TRIM',
    'ROUND', 'ROUNDUP', 'ROUNDDOWN', 'FLOOR', 'CEILING', 'ABS', 'POWER', 'SQRT', 'MOD',
    'TODAY', 'NOW', 'YEAR', 'MONTH', 'DAY', 'DATE', 'DATEDIF', 'WEEKDAY',
    'TRUE', 'FALSE', 'IFERROR', 'IFNA', 'ISERROR', 'ISNA', 'ISBLANK',
    'SUMIF', 'SUMIFS', 'COUNTIF', 'COUNTIFS', 'AVERAGEIF', 'AVERAGEIFS',
    'CHOOSE', 'SWITCH', 'INDIRECT', 'OFFSET', 'ROW', 'COLUMN', 'ROWS', 'COLUMNS',
    'COUNTA', 'COUNTBLANK', 'MEDIAN', 'MODE', 'STDEV', 'VAR',
}


class ExcelError(Exception):
    """Base class for Excel-like errors."""
    
    ERROR_CODES = {
        'REF': '#REF!',
        'VALUE': '#VALUE!',
        'NAME': '#NAME?',
        'DIV0': '#DIV/0!',
        'CIRCULAR': '#CIRCULAR!',
        'NULL': '#NULL!',
        'NUM': '#NUM!',
        'NA': '#N/A',
    }
    
    def __init__(self, error_type: str, message: str, details: str = None):
        self.error_type = error_type
        self.error_code = self.ERROR_CODES.get(error_type, '#ERROR!')
        self.message = message
        self.details = details
        super().__init__(f"{self.error_code} {message}")


class ErrorTranslator:
    """Translates errors to Excel-like error messages."""
    
    @staticmethod
    def translate(error: Exception, context: str = "") -> Dict[str, Any]:
        """Translate a Python exception to Excel-like error format."""
        error_str = str(error).lower()
        
        if isinstance(error, ExcelError):
            return {
                'error_code': error.error_code,
                'message': error.message,
                'details': error.details,
                'context': context,
            }
        
        if 'division by zero' in error_str or 'divide by zero' in error_str:
            return {'error_code': '#DIV/0!', 'message': 'Division by zero', 'details': str(error), 'context': context}
        elif 'not found' in error_str or 'invalid reference' in error_str:
            return {'error_code': '#REF!', 'message': 'Invalid reference', 'details': str(error), 'context': context}
        elif 'type' in error_str or 'cannot' in error_str:
            return {'error_code': '#VALUE!', 'message': 'Value error', 'details': str(error), 'context': context}
        elif 'name' in error_str or 'undefined' in error_str:
            return {'error_code': '#NAME?', 'message': 'Unknown name', 'details': str(error), 'context': context}
        elif 'circular' in error_str:
            return {'error_code': '#CIRCULAR!', 'message': 'Circular reference', 'details': str(error), 'context': context}
        else:
            return {'error_code': '#ERROR!', 'message': 'Formula error', 'details': str(error), 'context': context}


class DependencyGraph:
    """Builds and manages the dependency graph for formula columns."""
    
    def __init__(self):
        self.graph: Dict[str, Set[str]] = defaultdict(set)
        self.reverse_graph: Dict[str, Set[str]] = defaultdict(set)
        self.all_nodes: Set[str] = set()
    
    def add_dependency(self, column: str, depends_on: str):
        """Add a dependency: column depends on depends_on."""
        self.graph[column].add(depends_on)
        self.reverse_graph[depends_on].add(column)
        self.all_nodes.add(column)
        self.all_nodes.add(depends_on)
    
    def detect_circular_references(self) -> Optional[List[str]]:
        """Detect circular references using DFS."""
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {node: WHITE for node in self.all_nodes}
        
        def dfs(node: str, path: List[str]) -> Optional[List[str]]:
            color[node] = GRAY
            for neighbor in self.graph.get(node, set()):
                if neighbor not in color:
                    color[neighbor] = WHITE
                if color[neighbor] == GRAY:
                    cycle_start = path.index(neighbor) if neighbor in path else 0
                    return path[cycle_start:] + [node, neighbor]
                if color[neighbor] == WHITE:
                    result = dfs(neighbor, path + [node])
                    if result:
                        return result
            color[node] = BLACK
            return None
        
        for node in self.all_nodes:
            if color.get(node, WHITE) == WHITE:
                result = dfs(node, [])
                if result:
                    return result
        return None
    
    def topological_sort(self) -> List[str]:
        """Perform topological sort to get execution order."""
        cycle = self.detect_circular_references()
        if cycle:
            raise ExcelError('CIRCULAR', 'Circular reference detected', ' → '.join(cycle))
        
        in_degree = defaultdict(int)
        for node in self.all_nodes:
            for _ in self.graph.get(node, set()):
                in_degree[node] += 1
        
        queue = [node for node in self.all_nodes if in_degree[node] == 0]
        result = []
        
        while queue:
            node = queue.pop(0)
            result.append(node)
            for dependent in self.reverse_graph.get(node, set()):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        
        return result


class FormulaParser:
    """Parses Excel formulas and extracts column references."""
    
    @staticmethod
    def extract_column_references(formula: str, available_columns: Set[str]) -> Set[str]:
        """Extract column names referenced in the formula."""
        if not formula:
            return set()
        
        # Remove the leading = if present
        if formula.startswith('='):
            formula = formula[1:]
        
        references = set()
        
        # Find all potential identifiers (words that could be column names)
        # Match words that are not Excel functions
        words = re.findall(r'\b([A-Za-z_][A-Za-z0-9_]*)\b', formula)
        
        for word in words:
            # Skip Excel functions
            if word.upper() in EXCEL_FUNCTIONS:
                continue
            # Check if it matches an available column
            if word in available_columns:
                references.add(word)
        
        return references
    
    @staticmethod
    def convert_to_python_expr(formula: str, row_data: Dict[str, Any]) -> str:
        """Convert Excel formula to Python expression for evaluation."""
        if not formula:
            return "None"
        
        expr = formula[1:] if formula.startswith('=') else formula
        
        # Handle Excel IF function: IF(condition, true_val, false_val)
        # Convert to Python ternary: (true_val if condition else false_val)
        if_pattern = re.compile(r'\bIF\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)', re.IGNORECASE)
        while if_pattern.search(expr):
            expr = if_pattern.sub(r'((\2) if (\1) else (\3))', expr)
        
        # Handle comparison operators
        expr = expr.replace('<>', '!=')
        
        # Handle AND/OR
        expr = re.sub(r'\bAND\s*\(([^)]+)\)', r'(all([\1]))', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bOR\s*\(([^)]+)\)', r'(any([\1]))', expr, flags=re.IGNORECASE)
        
        # Handle basic Excel functions
        expr = re.sub(r'\bABS\s*\(', 'abs(', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bROUND\s*\(([^,]+),\s*(\d+)\)', r'round(\1, \2)', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bROUND\s*\(([^)]+)\)', r'round(\1)', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bPOWER\s*\(([^,]+),\s*([^)]+)\)', r'pow(\1, \2)', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bSQRT\s*\(', 'pow(', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bMOD\s*\(([^,]+),\s*([^)]+)\)', r'((\1) % (\2))', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bMAX\s*\(', 'max(', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bMIN\s*\(', 'min(', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bSUM\s*\(', 'sum([', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bAVERAGE\s*\(([^)]+)\)', r'(sum([\1])/len([\1]))', expr, flags=re.IGNORECASE)
        
        # Handle text functions - need to process before column replacement
        # CONCAT/CONCATENATE - join multiple values
        expr = re.sub(r'\bCONCAT\s*\(([^)]+)\)', r'_concat([\1])', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bCONCATENATE\s*\(([^)]+)\)', r'_concat([\1])', expr, flags=re.IGNORECASE)
        
        # LEFT/RIGHT/MID - string slicing
        expr = re.sub(r'\bLEFT\s*\(([^,]+),\s*(\d+)\)', r'_left(\1, \2)', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bRIGHT\s*\(([^,]+),\s*(\d+)\)', r'_right(\1, \2)', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bMID\s*\(([^,]+),\s*(\d+),\s*(\d+)\)', r'_mid(\1, \2, \3)', expr, flags=re.IGNORECASE)
        
        # LEN - string length
        expr = re.sub(r'\bLEN\s*\(([^)]+)\)', r'_len(\1)', expr, flags=re.IGNORECASE)
        
        # UPPER/LOWER/TRIM - string case/trim
        expr = re.sub(r'\bUPPER\s*\(([^)]+)\)', r'_upper(\1)', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bLOWER\s*\(([^)]+)\)', r'_lower(\1)', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bTRIM\s*\(([^)]+)\)', r'_trim(\1)', expr, flags=re.IGNORECASE)
        
        # FIND - find substring position
        expr = re.sub(r'\bFIND\s*\("([^"]+)",\s*([^)]+)\)', r'_find("\1", \2)', expr, flags=re.IGNORECASE)
        
        # SUBSTITUTE - replace text
        expr = re.sub(r'\bSUBSTITUTE\s*\(([^,]+),\s*"([^"]+)",\s*"([^"]+)"\)', r'_substitute(\1, "\2", "\3")', expr, flags=re.IGNORECASE)
        
        # TEXT - format number/date (simplified - just convert to string)
        expr = re.sub(r'\bTEXT\s*\(([^,]+),\s*"[^"]+"\)', r'str(\1)', expr, flags=re.IGNORECASE)
        
        # Lookup functions - these need special handling
        # VLOOKUP(lookup_value, table_array, return_column, [lookup_column], [range_lookup])
        # Enhanced version using column NAMES instead of column numbers
        def convert_vlookup(match):
            lookup_val = match.group(1).strip()
            table_arr = match.group(2).strip().strip('"')
            return_col = match.group(3).strip().strip('"')
            lookup_col = match.group(4).strip().strip('"') if match.group(4) else ''
            range_lookup = match.group(5)
            if range_lookup:
                range_lookup = 'True' if range_lookup.upper() in ('TRUE', '1') else 'False'
            else:
                range_lookup = 'False'
            if lookup_col:
                return f'_vlookup({lookup_val}, "{table_arr}", "{return_col}", "{lookup_col}", {range_lookup})'
            else:
                return f'_vlookup({lookup_val}, "{table_arr}", "{return_col}", None, {range_lookup})'
        
        # Full pattern: VLOOKUP(value, "table", "return_col", "lookup_col", TRUE/FALSE)
        expr = re.sub(
            r'\bVLOOKUP\s*\(\s*([^,]+)\s*,\s*"?([^",]+)"?\s*,\s*"?([^",]+)"?\s*,\s*"?([^",]+)"?\s*,\s*(TRUE|FALSE|0|1)\s*\)',
            convert_vlookup,
            expr, flags=re.IGNORECASE
        )
        
        # Pattern with lookup_col but no range_lookup: VLOOKUP(value, "table", "return_col", "lookup_col")
        def convert_vlookup_4args(match):
            lookup_val = match.group(1).strip()
            table_arr = match.group(2).strip().strip('"')
            return_col = match.group(3).strip().strip('"')
            lookup_col = match.group(4).strip().strip('"')
            return f'_vlookup({lookup_val}, "{table_arr}", "{return_col}", "{lookup_col}", False)'
        
        expr = re.sub(
            r'\bVLOOKUP\s*\(\s*([^,]+)\s*,\s*"?([^",]+)"?\s*,\s*"?([^",]+)"?\s*,\s*"?([^",]+)"?\s*\)',
            convert_vlookup_4args,
            expr, flags=re.IGNORECASE
        )
        
        # Simplified VLOOKUP pattern (3 args): VLOOKUP(value, "table", "return_col")
        def convert_vlookup_simple(match):
            lookup_val = match.group(1).strip()
            table_arr = match.group(2).strip().strip('"')
            return_col = match.group(3).strip().strip('"')
            return f'_vlookup({lookup_val}, "{table_arr}", "{return_col}", None, False)'
        
        expr = re.sub(
            r'\bVLOOKUP\s*\(\s*([^,]+)\s*,\s*"?([^",]+)"?\s*,\s*"?([^",]+)"?\s*\)',
            convert_vlookup_simple,
            expr, flags=re.IGNORECASE
        )
        
        # HLOOKUP(lookup_value, table_array, row_index, [range_lookup])
        def convert_hlookup(match):
            lookup_val = match.group(1).strip()
            table_arr = match.group(2).strip().strip('"')
            row_idx = match.group(3).strip()
            range_lookup = match.group(4)
            if range_lookup:
                range_lookup = 'True' if range_lookup.upper() in ('TRUE', '1') else 'False'
            else:
                range_lookup = 'False'
            return f'_hlookup({lookup_val}, "{table_arr}", {row_idx}, {range_lookup})'
        
        expr = re.sub(
            r'\bHLOOKUP\s*\(\s*([^,]+)\s*,\s*"?([^",]+)"?\s*,\s*(\d+)\s*(?:,\s*(TRUE|FALSE|0|1))?\s*\)',
            convert_hlookup,
            expr, flags=re.IGNORECASE
        )
        def convert_hlookup_simple(match):
            lookup_val = match.group(1).strip()
            table_arr = match.group(2).strip().strip('"')
            row_idx = match.group(3).strip()
            return f'_hlookup({lookup_val}, "{table_arr}", {row_idx}, False)'
        
        expr = re.sub(
            r'\bHLOOKUP\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*(\d+)\s*\)',
            convert_hlookup_simple,
            expr, flags=re.IGNORECASE
        )
        
        # XLOOKUP(lookup_value, lookup_array, return_array, [if_not_found], [match_mode], [search_mode])
        # Use a function to handle XLOOKUP conversion properly
        def convert_xlookup(match):
            lookup_val = match.group(1).strip()
            lookup_arr = match.group(2).strip().strip('"')
            return_arr = match.group(3).strip().strip('"')
            if_not_found = match.group(4).strip().strip('"') if match.group(4) else '#N/A'
            match_mode = match.group(5) if match.group(5) else '0'
            search_mode = match.group(6) if match.group(6) else '1'
            return f'_xlookup({lookup_val}, "{lookup_arr}", "{return_arr}", "{if_not_found}", {match_mode}, {search_mode})'
        
        expr = re.sub(
            r'\bXLOOKUP\s*\(\s*([^,]+)\s*,\s*"?([^",]+)"?\s*,\s*"?([^",]+)"?\s*(?:,\s*"?([^",]*)"?)?\s*(?:,\s*(-?\d+))?\s*(?:,\s*(-?\d+))?\s*\)',
            convert_xlookup,
            expr, flags=re.IGNORECASE
        )
        # Simplified XLOOKUP pattern (3 args only)
        def convert_xlookup_simple(match):
            lookup_val = match.group(1).strip()
            lookup_arr = match.group(2).strip().strip('"')
            return_arr = match.group(3).strip().strip('"')
            return f'_xlookup({lookup_val}, "{lookup_arr}", "{return_arr}", "#N/A", 0, 1)'
        
        expr = re.sub(
            r'\bXLOOKUP\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,)]+)\s*\)',
            convert_xlookup_simple,
            expr, flags=re.IGNORECASE
        )
        
        # INDEX(array, row_num, [col_num])
        expr = re.sub(
            r'\bINDEX\s*\(\s*"?([^",]+)"?\s*,\s*(\d+)\s*(?:,\s*(\d+))?\s*\)',
            r'_index("\1", \2, \3 if "\3" else 1)',
            expr, flags=re.IGNORECASE
        )
        expr = re.sub(
            r'\bINDEX\s*\(\s*([^,]+)\s*,\s*(\d+)\s*\)',
            r'_index(\1, \2, 1)',
            expr, flags=re.IGNORECASE
        )
        
        # MATCH(lookup_value, lookup_array, [match_type])
        expr = re.sub(
            r'\bMATCH\s*\(\s*([^,]+)\s*,\s*"?([^",]+)"?\s*(?:,\s*(-?\d+))?\s*\)',
            r'_match(\1, "\2", \3 if "\3" else 1)',
            expr, flags=re.IGNORECASE
        )
        expr = re.sub(
            r'\bMATCH\s*\(\s*([^,]+)\s*,\s*([^,)]+)\s*\)',
            r'_match(\1, \2, 1)',
            expr, flags=re.IGNORECASE
        )
        
        # LOOKUP(lookup_value, lookup_vector, [result_vector])
        expr = re.sub(
            r'\bLOOKUP\s*\(\s*([^,]+)\s*,\s*"?([^",]+)"?\s*(?:,\s*"?([^",]+)"?)?\s*\)',
            r'_lookup(\1, "\2", "\3" if "\3" else None)',
            expr, flags=re.IGNORECASE
        )
        
        # COUNTIF(range, criteria)
        expr = re.sub(
            r'\bCOUNTIF\s*\(\s*"?([^",]+)"?\s*,\s*"?([^")]+)"?\s*\)',
            r'_countif("\1", "\2")',
            expr, flags=re.IGNORECASE
        )
        expr = re.sub(
            r'\bCOUNTIF\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
            r'_countif(\1, \2)',
            expr, flags=re.IGNORECASE
        )
        
        # SUMIF(range, criteria, [sum_range])
        expr = re.sub(
            r'\bSUMIF\s*\(\s*"?([^",]+)"?\s*,\s*"?([^",]+)"?\s*(?:,\s*"?([^",]+)"?)?\s*\)',
            r'_sumif("\1", "\2", "\3" if "\3" else None)',
            expr, flags=re.IGNORECASE
        )
        expr = re.sub(
            r'\bSUMIF\s*\(\s*([^,]+)\s*,\s*([^,)]+)\s*\)',
            r'_sumif(\1, \2, None)',
            expr, flags=re.IGNORECASE
        )
        
        # AVERAGEIF(range, criteria, [average_range])
        expr = re.sub(
            r'\bAVERAGEIF\s*\(\s*"?([^",]+)"?\s*,\s*"?([^",]+)"?\s*(?:,\s*"?([^",]+)"?)?\s*\)',
            r'_averageif("\1", "\2", "\3" if "\3" else None)',
            expr, flags=re.IGNORECASE
        )
        expr = re.sub(
            r'\bAVERAGEIF\s*\(\s*([^,]+)\s*,\s*([^,)]+)\s*\)',
            r'_averageif(\1, \2, None)',
            expr, flags=re.IGNORECASE
        )
        
        # IFERROR(value, value_if_error)
        expr = re.sub(
            r'\bIFERROR\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
            r'_iferror(\1, \2)',
            expr, flags=re.IGNORECASE
        )
        
        # ISBLANK(value)
        expr = re.sub(r'\bISBLANK\s*\(([^)]+)\)', r'_isblank(\1)', expr, flags=re.IGNORECASE)
        
        # COUNTA(value1, value2, ...)
        expr = re.sub(r'\bCOUNTA\s*\(([^)]+)\)', r'_counta(\1)', expr, flags=re.IGNORECASE)
        
        # Close SUM brackets
        if 'sum([' in expr:
            expr = re.sub(r'sum\(\[([^)]+)\)', r'sum([\1])', expr)
        
        # Handle TRUE/FALSE
        expr = re.sub(r'\bTRUE\b', 'True', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bFALSE\b', 'False', expr, flags=re.IGNORECASE)
        
        # Replace column names with their values from row_data
        # BUT preserve quoted strings - don't replace column names inside quotes
        
        # Step 1: Extract all quoted strings and replace with placeholders
        quoted_strings = []
        def save_quoted(match):
            quoted_strings.append(match.group(0))
            return f'__QUOTED_{len(quoted_strings) - 1}__'
        
        # Match both single and double quoted strings
        expr = re.sub(r'"[^"]*"', save_quoted, expr)
        expr = re.sub(r"'[^']*'", save_quoted, expr)
        
        # Step 2: Replace column names (longest first to avoid partial replacements)
        columns = sorted(row_data.keys(), key=len, reverse=True)
        for col in columns:
            value = row_data[col]
            # Handle None/NaN as 0 for numeric operations
            if pd.isna(value):
                value = 0
            # Quote strings
            if isinstance(value, str):
                replacement = f'"{value}"'
            else:
                replacement = str(value)
            # Use word boundary replacement
            expr = re.sub(rf'\b{re.escape(col)}\b', replacement, expr)
        
        # Step 3: Restore quoted strings
        for i, quoted in enumerate(quoted_strings):
            expr = expr.replace(f'__QUOTED_{i}__', quoted)
        
        return expr


class WorkbookBuilder:
    """Builds a virtual Excel workbook from pandas DataFrames."""
    
    def __init__(self):
        self.sheets: Dict[str, pd.DataFrame] = {}
    
    def add_sheet(self, name: str, df: pd.DataFrame):
        """Add a DataFrame as a sheet."""
        # Keep the name as-is since we use "FileLabel.SheetName" format intentionally
        self.sheets[name] = df.copy()
    
    def get_all_columns(self, sheet: str = None) -> Set[str]:
        """Get all column names, optionally for a specific sheet."""
        if sheet:
            # Try exact match first
            if sheet in self.sheets:
                return set(self.sheets[sheet].columns)
            # Try case-insensitive match
            sheet_lower = sheet.lower()
            for key in self.sheets:
                if key.lower() == sheet_lower:
                    return set(self.sheets[key].columns)
        
        all_cols = set()
        for df in self.sheets.values():
            all_cols.update(df.columns)
        return all_cols
    
    def get_sheet_data(self, sheet: str) -> pd.DataFrame:
        """Get DataFrame for a sheet."""
        # Try exact match first
        if sheet in self.sheets:
            return self.sheets[sheet]
        
        # Try case-insensitive match
        sheet_lower = sheet.lower()
        for key in self.sheets:
            if key.lower() == sheet_lower:
                return self.sheets[key]
        
        # Not found
        available = list(self.sheets.keys())
        raise ExcelError('REF', f"Sheet '{sheet}' not found. Available sheets: {available}")
    
    def add_column(self, sheet: str, column_name: str, values: List[Any]):
        """Add a new column to a sheet."""
        # Try exact match first
        if sheet in self.sheets:
            self.sheets[sheet][column_name] = values
            return
        
        # Try case-insensitive match
        sheet_lower = sheet.lower()
        for key in self.sheets:
            if key.lower() == sheet_lower:
                self.sheets[key][column_name] = values
                return
        
        # Not found
        available = list(self.sheets.keys())
        raise ExcelError('REF', f"Sheet '{sheet}' not found. Available sheets: {available}")


class LookupFunctions:
    """Excel lookup functions that need access to entire sheets."""
    
    def __init__(self, workbook: WorkbookBuilder):
        self.workbook = workbook
    
    def vlookup(self, lookup_value, table_array, return_col, lookup_col=None, range_lookup=False):
        """
        VLOOKUP - Vertical lookup (Enhanced version using column names)
        Searches for a value in a column and returns a value from another column.
        
        Args:
            lookup_value: The value to search for
            table_array: Sheet name in format "FileLabel.SheetName" (e.g., "File1.Stock")
            return_col: Column NAME to return from (e.g., "Closing")
            lookup_col: Column NAME to search in (optional, defaults to first column)
            range_lookup: False for exact match (default), True for approximate match
        """
        try:
            # Get the data as DataFrame
            if isinstance(table_array, str):
                table_array = table_array.strip()
                df = self.workbook.get_sheet_data(table_array)
            elif isinstance(table_array, pd.DataFrame):
                df = table_array
            else:
                return '#VALUE!'
            
            if df.empty:
                return '#REF!'
            
            # Get lookup column (first column if not specified)
            if lookup_col:
                lookup_col = str(lookup_col).strip()
                if lookup_col not in df.columns:
                    # Try case-insensitive match
                    lookup_col_lower = lookup_col.lower()
                    found = False
                    for c in df.columns:
                        if str(c).lower() == lookup_col_lower:
                            lookup_col = c
                            found = True
                            break
                    if not found:
                        return '#REF!'
                search_col = df[lookup_col]
            else:
                search_col = df.iloc[:, 0]
            
            # Get return column by name
            return_col = str(return_col).strip()
            if return_col not in df.columns:
                # Try case-insensitive match
                return_col_lower = return_col.lower()
                found = False
                for c in df.columns:
                    if str(c).lower() == return_col_lower:
                        return_col = c
                        found = True
                        break
                if not found:
                    return '#REF!'
            
            # Handle None/NaN lookup value
            if lookup_value is None or (isinstance(lookup_value, float) and pd.isna(lookup_value)):
                return '#N/A'
            
            # Convert lookup_value for comparison
            lookup_str = str(lookup_value).strip().lower()
            try:
                lookup_num = float(lookup_value)
                has_numeric = True
            except:
                lookup_num = None
                has_numeric = False
            
            # Search for the value
            if range_lookup:
                # Approximate match - find largest value <= lookup_value
                sorted_indices = search_col.sort_values().index
                match_idx = None
                for idx in sorted_indices:
                    val = search_col[idx]
                    if pd.isna(val):
                        continue
                    if val <= lookup_value:
                        match_idx = idx
                    else:
                        break
                if match_idx is not None:
                    result = df.loc[match_idx, return_col]
                    return '' if pd.isna(result) else str(result)
            else:
                # Exact match - iterate through rows
                for idx in range(len(df)):
                    val = search_col.iloc[idx]
                    if pd.isna(val):
                        continue
                    
                    # Try numeric comparison
                    if has_numeric:
                        try:
                            if float(val) == lookup_num:
                                result = df.iloc[idx][return_col]
                                return '' if pd.isna(result) else str(result)
                        except:
                            pass
                    
                    # String comparison (case-insensitive)
                    if str(val).strip().lower() == lookup_str:
                        result = df.iloc[idx][return_col]
                        return '' if pd.isna(result) else str(result)
            
            return '#N/A'
            
        except Exception as e:
            return f'#ERROR!'
    
    def hlookup(self, lookup_value, table_array, row_index, range_lookup=False):
        """
        HLOOKUP - Horizontal lookup
        Searches for a value in the first row and returns a value from another row.
        
        Args:
            lookup_value: The value to search for
            table_array: Can be a sheet name (str) or a DataFrame
            row_index: The row number to return from (1-based)
            range_lookup: False for exact match (default), True for approximate match
        """
        try:
            # Get the data as DataFrame
            if isinstance(table_array, str):
                df = self.workbook.get_sheet_data(table_array)
            elif isinstance(table_array, pd.DataFrame):
                df = table_array
            else:
                return '#VALUE!'
            
            if df.empty or row_index < 1 or row_index > len(df):
                return '#REF!'
            
            row_index = int(row_index) - 1  # Convert to 0-based
            
            # Search in first row (column headers or first data row)
            first_row = df.iloc[0].values
            
            for col_idx, val in enumerate(first_row):
                if val == lookup_value or str(val) == str(lookup_value):
                    return str(df.iloc[row_index, col_idx])
            
            return '#N/A'
            
        except Exception as e:
            return f'#ERROR!'
    
    def xlookup(self, lookup_value, lookup_array, return_array, if_not_found='#N/A', match_mode=0, search_mode=1):
        """
        XLOOKUP - Modern lookup function
        Searches for a value in a range and returns a corresponding value.
        
        Args:
            lookup_value: The value to search for
            lookup_array: Column name, sheet.column, or array to search in
            return_array: Column name, sheet.column, or array to return from
            if_not_found: Value to return if no match (default '#N/A')
            match_mode: 0=exact match (default), -1=next smaller, 1=next larger, 2=wildcard
            search_mode: 1=first to last (default), -1=last to first
        """
        try:
            # Convert match_mode and search_mode to int
            match_mode = int(match_mode) if match_mode is not None else 0
            search_mode = int(search_mode) if search_mode is not None else 1
            
            # Parse lookup_array - can be "sheet.column" or just column name
            lookup_col = None
            if isinstance(lookup_array, str):
                lookup_array = lookup_array.strip()
                if '.' in lookup_array:
                    # Split on the LAST dot to get sheet.column
                    parts = lookup_array.rsplit('.', 1)
                    if len(parts) == 2:
                        sheet_name, col_name = parts
                        try:
                            df = self.workbook.get_sheet_data(sheet_name)
                            if col_name in df.columns:
                                lookup_col = df[col_name]
                            else:
                                # Try case-insensitive column match
                                col_lower = col_name.lower()
                                for c in df.columns:
                                    if str(c).lower() == col_lower:
                                        lookup_col = df[c]
                                        break
                        except:
                            pass
                
                if lookup_col is None:
                    # Search in all sheets
                    for sheet_name, df in self.workbook.sheets.items():
                        if lookup_array in df.columns:
                            lookup_col = df[lookup_array]
                            break
                        # Try case-insensitive
                        for c in df.columns:
                            if str(c).lower() == lookup_array.lower():
                                lookup_col = df[c]
                                break
                        if lookup_col is not None:
                            break
                
                if lookup_col is None:
                    return '#REF!'
            elif isinstance(lookup_array, pd.Series):
                lookup_col = lookup_array
            else:
                return '#VALUE!'
            
            # Parse return_array similarly
            return_col = None
            if isinstance(return_array, str):
                return_array = return_array.strip()
                if '.' in return_array:
                    parts = return_array.rsplit('.', 1)
                    if len(parts) == 2:
                        sheet_name, col_name = parts
                        try:
                            df = self.workbook.get_sheet_data(sheet_name)
                            if col_name in df.columns:
                                return_col = df[col_name]
                            else:
                                col_lower = col_name.lower()
                                for c in df.columns:
                                    if str(c).lower() == col_lower:
                                        return_col = df[c]
                                        break
                        except:
                            pass
                
                if return_col is None:
                    for sheet_name, df in self.workbook.sheets.items():
                        if return_array in df.columns:
                            return_col = df[return_array]
                            break
                        for c in df.columns:
                            if str(c).lower() == return_array.lower():
                                return_col = df[c]
                                break
                        if return_col is not None:
                            break
                
                if return_col is None:
                    return '#REF!'
            elif isinstance(return_array, pd.Series):
                return_col = return_array
            else:
                return '#VALUE!'
            
            # Ensure same length
            if len(lookup_col) != len(return_col):
                return '#VALUE!'
            
            # Handle None/NaN lookup value
            if lookup_value is None or (isinstance(lookup_value, float) and pd.isna(lookup_value)):
                return str(if_not_found) if if_not_found else '#N/A'
            
            # Convert lookup_value to string for comparison
            lookup_str = str(lookup_value).strip()
            
            # Also try as number if possible
            try:
                lookup_num = float(lookup_value)
                has_numeric = True
            except:
                lookup_num = None
                has_numeric = False
            
            # Search for the value
            indices = range(len(lookup_col)) if search_mode >= 0 else reversed(range(len(lookup_col)))
            
            for idx in indices:
                val = lookup_col.iloc[idx]
                
                # Skip NaN values in lookup column
                if pd.isna(val):
                    continue
                
                if match_mode == 0:  # Exact match
                    # Try numeric comparison first
                    if has_numeric:
                        try:
                            if float(val) == lookup_num:
                                result = return_col.iloc[idx]
                                return '' if pd.isna(result) else str(result)
                        except:
                            pass
                    
                    # String comparison (case-insensitive)
                    if str(val).strip().lower() == lookup_str.lower():
                        result = return_col.iloc[idx]
                        return '' if pd.isna(result) else str(result)
                        
                elif match_mode == 2:  # Wildcard match
                    import fnmatch
                    if fnmatch.fnmatch(str(val).lower(), lookup_str.lower()):
                        result = return_col.iloc[idx]
                        return '' if pd.isna(result) else str(result)
            
            return str(if_not_found) if if_not_found else '#N/A'
            
        except Exception as e:
            return '#ERROR!'
    
    def index_func(self, array, row_num, col_num=1):
        """
        INDEX - Returns a value at a given position in an array
        
        Args:
            array: Sheet name or DataFrame
            row_num: Row number (1-based)
            col_num: Column number (1-based, default 1)
        """
        try:
            if isinstance(array, str):
                df = self.workbook.get_sheet_data(array)
            elif isinstance(array, pd.DataFrame):
                df = array
            else:
                return '#VALUE!'
            
            row_num = int(row_num) - 1  # Convert to 0-based
            col_num = int(col_num) - 1
            
            if row_num < 0 or row_num >= len(df):
                return '#REF!'
            if col_num < 0 or col_num >= len(df.columns):
                return '#REF!'
            
            return str(df.iloc[row_num, col_num])
            
        except Exception as e:
            return '#ERROR!'
    
    def match_func(self, lookup_value, lookup_array, match_type=1):
        """
        MATCH - Returns the position of a value in an array
        
        Args:
            lookup_value: The value to find
            lookup_array: Column name or sheet.column to search in
            match_type: 1=less than (default), 0=exact, -1=greater than
        """
        try:
            # Parse lookup_array
            if isinstance(lookup_array, str):
                if '.' in lookup_array:
                    sheet_name, col_name = lookup_array.rsplit('.', 1)
                    df = self.workbook.get_sheet_data(sheet_name)
                    lookup_col = df[col_name] if col_name in df.columns else None
                else:
                    lookup_col = None
                    for sheet_name, df in self.workbook.sheets.items():
                        if lookup_array in df.columns:
                            lookup_col = df[lookup_array]
                            break
                
                if lookup_col is None:
                    return '#REF!'
            elif isinstance(lookup_array, pd.Series):
                lookup_col = lookup_array
            else:
                return '#VALUE!'
            
            match_type = int(match_type)
            
            if match_type == 0:  # Exact match
                for idx, val in enumerate(lookup_col):
                    if val == lookup_value or str(val) == str(lookup_value):
                        return idx + 1  # Return 1-based position
            elif match_type == 1:  # Less than or equal (assumes sorted ascending)
                last_match = None
                for idx, val in enumerate(lookup_col):
                    if val <= lookup_value:
                        last_match = idx + 1
                    else:
                        break
                if last_match:
                    return last_match
            elif match_type == -1:  # Greater than or equal (assumes sorted descending)
                for idx, val in enumerate(lookup_col):
                    if val >= lookup_value:
                        return idx + 1
            
            return '#N/A'
            
        except Exception as e:
            return '#ERROR!'
    
    def lookup(self, lookup_value, lookup_vector, result_vector=None):
        """
        LOOKUP - Simple lookup function
        
        Args:
            lookup_value: The value to find
            lookup_vector: Column to search in (sheet.column format)
            result_vector: Column to return from (sheet.column format, optional)
        """
        try:
            # Parse lookup_vector
            if isinstance(lookup_vector, str):
                if '.' in lookup_vector:
                    sheet_name, col_name = lookup_vector.rsplit('.', 1)
                    df = self.workbook.get_sheet_data(sheet_name)
                    lookup_col = df[col_name] if col_name in df.columns else None
                else:
                    lookup_col = None
                    for sheet_name, df in self.workbook.sheets.items():
                        if lookup_vector in df.columns:
                            lookup_col = df[lookup_vector]
                            break
            else:
                return '#VALUE!'
            
            if lookup_col is None:
                return '#REF!'
            
            # Determine result column
            if result_vector is None:
                result_col = lookup_col
            elif isinstance(result_vector, str):
                if '.' in result_vector:
                    sheet_name, col_name = result_vector.rsplit('.', 1)
                    df = self.workbook.get_sheet_data(sheet_name)
                    result_col = df[col_name] if col_name in df.columns else None
                else:
                    result_col = None
                    for sheet_name, df in self.workbook.sheets.items():
                        if result_vector in df.columns:
                            result_col = df[result_vector]
                            break
            else:
                return '#VALUE!'
            
            if result_col is None:
                return '#REF!'
            
            # Find the value (assumes sorted, finds largest <= lookup_value)
            last_match_idx = None
            for idx, val in enumerate(lookup_col):
                if val <= lookup_value:
                    last_match_idx = idx
                else:
                    break
            
            if last_match_idx is not None:
                return str(result_col.iloc[last_match_idx])
            
            return '#N/A'
            
        except Exception as e:
            return '#ERROR!'
    
    def countif(self, range_ref, criteria):
        """COUNTIF - Count cells that meet a criteria."""
        try:
            # Parse range reference
            if isinstance(range_ref, str):
                if '.' in range_ref:
                    sheet_name, col_name = range_ref.rsplit('.', 1)
                    df = self.workbook.get_sheet_data(sheet_name)
                    data_col = df[col_name] if col_name in df.columns else None
                else:
                    data_col = None
                    for sheet_name, df in self.workbook.sheets.items():
                        if range_ref in df.columns:
                            data_col = df[range_ref]
                            break
            else:
                return '#VALUE!'
            
            if data_col is None:
                return '#REF!'
            
            # Parse criteria (supports >, <, >=, <=, <>)
            criteria_str = str(criteria)
            if criteria_str.startswith('>='):
                return sum(1 for v in data_col if v >= float(criteria_str[2:]))
            elif criteria_str.startswith('<='):
                return sum(1 for v in data_col if v <= float(criteria_str[2:]))
            elif criteria_str.startswith('<>'):
                return sum(1 for v in data_col if str(v) != criteria_str[2:])
            elif criteria_str.startswith('>'):
                return sum(1 for v in data_col if v > float(criteria_str[1:]))
            elif criteria_str.startswith('<'):
                return sum(1 for v in data_col if v < float(criteria_str[1:]))
            else:
                # Exact match
                return sum(1 for v in data_col if v == criteria or str(v) == criteria_str)
                
        except Exception as e:
            return '#ERROR!'
    
    def sumif(self, range_ref, criteria, sum_range=None):
        """SUMIF - Sum cells that meet a criteria."""
        try:
            # Parse range reference
            if isinstance(range_ref, str):
                if '.' in range_ref:
                    sheet_name, col_name = range_ref.rsplit('.', 1)
                    df = self.workbook.get_sheet_data(sheet_name)
                    data_col = df[col_name] if col_name in df.columns else None
                else:
                    data_col = None
                    for sheet_name, df in self.workbook.sheets.items():
                        if range_ref in df.columns:
                            data_col = df[range_ref]
                            break
            else:
                return '#VALUE!'
            
            if data_col is None:
                return '#REF!'
            
            # Parse sum_range if provided
            if sum_range:
                if isinstance(sum_range, str):
                    if '.' in sum_range:
                        sheet_name, col_name = sum_range.rsplit('.', 1)
                        df = self.workbook.get_sheet_data(sheet_name)
                        sum_col = df[col_name] if col_name in df.columns else None
                    else:
                        sum_col = None
                        for sheet_name, df in self.workbook.sheets.items():
                            if sum_range in df.columns:
                                sum_col = df[sum_range]
                                break
                else:
                    return '#VALUE!'
                
                if sum_col is None:
                    return '#REF!'
            else:
                sum_col = data_col
            
            # Parse criteria
            criteria_str = str(criteria)
            total = 0
            
            for idx, v in enumerate(data_col):
                match = False
                if criteria_str.startswith('>='):
                    match = v >= float(criteria_str[2:])
                elif criteria_str.startswith('<='):
                    match = v <= float(criteria_str[2:])
                elif criteria_str.startswith('<>'):
                    match = str(v) != criteria_str[2:]
                elif criteria_str.startswith('>'):
                    match = v > float(criteria_str[1:])
                elif criteria_str.startswith('<'):
                    match = v < float(criteria_str[1:])
                else:
                    match = v == criteria or str(v) == criteria_str
                
                if match:
                    try:
                        total += float(sum_col.iloc[idx])
                    except (ValueError, TypeError):
                        pass
            
            return total
                
        except Exception as e:
            return '#ERROR!'
    
    def averageif(self, range_ref, criteria, average_range=None):
        """AVERAGEIF - Average cells that meet a criteria."""
        try:
            # Similar to SUMIF but calculates average
            if isinstance(range_ref, str):
                if '.' in range_ref:
                    sheet_name, col_name = range_ref.rsplit('.', 1)
                    df = self.workbook.get_sheet_data(sheet_name)
                    data_col = df[col_name] if col_name in df.columns else None
                else:
                    data_col = None
                    for sheet_name, df in self.workbook.sheets.items():
                        if range_ref in df.columns:
                            data_col = df[range_ref]
                            break
            else:
                return '#VALUE!'
            
            if data_col is None:
                return '#REF!'
            
            # Parse average_range if provided
            if average_range:
                if isinstance(average_range, str):
                    if '.' in average_range:
                        sheet_name, col_name = average_range.rsplit('.', 1)
                        df = self.workbook.get_sheet_data(sheet_name)
                        avg_col = df[col_name] if col_name in df.columns else None
                    else:
                        avg_col = None
                        for sheet_name, df in self.workbook.sheets.items():
                            if average_range in df.columns:
                                avg_col = df[average_range]
                                break
                else:
                    return '#VALUE!'
                
                if avg_col is None:
                    return '#REF!'
            else:
                avg_col = data_col
            
            # Parse criteria
            criteria_str = str(criteria)
            values = []
            
            for idx, v in enumerate(data_col):
                match = False
                if criteria_str.startswith('>='):
                    match = v >= float(criteria_str[2:])
                elif criteria_str.startswith('<='):
                    match = v <= float(criteria_str[2:])
                elif criteria_str.startswith('<>'):
                    match = str(v) != criteria_str[2:]
                elif criteria_str.startswith('>'):
                    match = v > float(criteria_str[1:])
                elif criteria_str.startswith('<'):
                    match = v < float(criteria_str[1:])
                else:
                    match = v == criteria or str(v) == criteria_str
                
                if match:
                    try:
                        values.append(float(avg_col.iloc[idx]))
                    except (ValueError, TypeError):
                        pass
            
            if values:
                return sum(values) / len(values)
            return '#DIV/0!'
                
        except Exception as e:
            return '#ERROR!'
    
    def iferror(self, value, value_if_error):
        """IFERROR - Returns value_if_error if value is an error, otherwise returns value."""
        if isinstance(value, str) and value.startswith('#'):
            return value_if_error
        return value
    
    def isblank(self, value):
        """ISBLANK - Returns TRUE if value is blank/empty."""
        if value is None or (isinstance(value, str) and value.strip() == ''):
            return True
        if pd.isna(value):
            return True
        return False
    
    def counta(self, *args):
        """COUNTA - Counts non-empty cells."""
        count = 0
        for arg in args:
            if isinstance(arg, (list, tuple, pd.Series)):
                for v in arg:
                    if v is not None and not (isinstance(v, str) and v.strip() == '') and not pd.isna(v):
                        count += 1
            else:
                if arg is not None and not (isinstance(arg, str) and arg.strip() == '') and not pd.isna(arg):
                    count += 1
        return count


class FormulaEngine:
    """Main formula evaluation engine."""
    
    def __init__(self, workbook: WorkbookBuilder):
        self.workbook = workbook
        self.computed_columns: Dict[str, List[Any]] = {}
        self.lookup_functions = LookupFunctions(workbook)
    
    def evaluate_formula(self, formula: str, sheet: str) -> List[Any]:
        """Evaluate a formula for all rows in a sheet."""
        df = self.workbook.get_sheet_data(sheet)
        results = []
        
        # Get available columns (including computed ones FOR THIS SHEET)
        # Computed columns are stored as "sheet.column" keys
        sheet_computed_cols = {}
        for full_key, values in self.computed_columns.items():
            if '.' in full_key:
                col_sheet, col_name = full_key.rsplit('.', 1)
                if col_sheet == sheet:
                    sheet_computed_cols[col_name] = values
            else:
                # Legacy: column without sheet prefix
                sheet_computed_cols[full_key] = values
        
        available_columns = set(df.columns) | set(sheet_computed_cols.keys())
        
        for idx in range(len(df)):
            # Build row data dictionary
            row_data = {}
            for col in df.columns:
                row_data[col] = df.iloc[idx][col]
            # Add computed columns FOR THIS SHEET ONLY
            for col, values in sheet_computed_cols.items():
                if idx < len(values):
                    row_data[col] = values[idx]
            
            # Evaluate for this row
            result = self._evaluate_row(formula, row_data, available_columns, idx)
            results.append(result)
        
        return results
    
    def _evaluate_row(self, formula: str, row_data: Dict[str, Any], available_columns: Set[str], row_idx: int) -> Any:
        """Evaluate formula for a single row."""
        try:
            # Convert formula to Python expression
            python_expr = FormulaParser.convert_to_python_expr(formula, row_data)
            
            # Evaluate using Python's eval (safe for numeric expressions)
            # Create a safe namespace
            def _concat(*args):
                """Concatenate multiple values into a string."""
                # Handle case where a list is passed as single argument
                if len(args) == 1 and isinstance(args[0], (list, tuple)):
                    items = args[0]
                else:
                    items = args
                return ''.join(str(v) if v is not None else '' for v in items)
            
            def _left(value, n):
                """Get left n characters."""
                return str(value)[:int(n)] if value is not None else ''
            
            def _right(value, n):
                """Get right n characters."""
                s = str(value) if value is not None else ''
                return s[-int(n):] if int(n) > 0 else ''
            
            def _mid(value, start, length):
                """Get substring starting at start (1-based) with given length."""
                s = str(value) if value is not None else ''
                start_idx = int(start) - 1  # Convert to 0-based
                end_idx = start_idx + int(length)
                return s[start_idx:end_idx] if start_idx >= 0 else ''
            
            def _len(value):
                """Get string length."""
                return len(str(value)) if value is not None else 0
            
            def _upper(value):
                """Convert to uppercase."""
                return str(value).upper() if value is not None else ''
            
            def _lower(value):
                """Convert to lowercase."""
                return str(value).lower() if value is not None else ''
            
            def _trim(value):
                """Remove leading/trailing whitespace."""
                return str(value).strip() if value is not None else ''
            
            def _find(search_text, value):
                """Find position of search_text in value (1-based, 0 if not found)."""
                s = str(value) if value is not None else ''
                pos = s.find(search_text)
                return pos + 1 if pos >= 0 else 0
            
            def _substitute(value, old_text, new_text):
                """Replace old_text with new_text in value."""
                return str(value).replace(old_text, new_text) if value is not None else ''
            
            safe_dict = {
                'abs': abs,
                'round': round,
                'pow': pow,
                'max': max,
                'min': min,
                'sum': sum,
                'len': len,
                'all': all,
                'any': any,
                'True': True,
                'False': False,
                'None': None,
                'str': str,
                'float': float,
                'int': int,
                # Text functions
                '_concat': _concat,
                '_left': _left,
                '_right': _right,
                '_mid': _mid,
                '_len': _len,
                '_upper': _upper,
                '_lower': _lower,
                '_trim': _trim,
                '_find': _find,
                '_substitute': _substitute,
                # Lookup functions
                '_vlookup': self.lookup_functions.vlookup,
                '_hlookup': self.lookup_functions.hlookup,
                '_xlookup': self.lookup_functions.xlookup,
                '_index': self.lookup_functions.index_func,
                '_match': self.lookup_functions.match_func,
                '_lookup': self.lookup_functions.lookup,
                '_countif': self.lookup_functions.countif,
                '_sumif': self.lookup_functions.sumif,
                '_averageif': self.lookup_functions.averageif,
                '_iferror': self.lookup_functions.iferror,
                '_isblank': self.lookup_functions.isblank,
                '_counta': self.lookup_functions.counta,
            }
            
            result = eval(python_expr, {"__builtins__": {}}, safe_dict)
            
            # Handle numpy types first
            if isinstance(result, (np.integer, np.floating)):
                result = result.item()
            elif isinstance(result, np.bool_):
                result = bool(result)
            
            # Convert all results to strings (like Excel does when exporting)
            # Handle special cases:
            if result is None:
                return ''
            elif isinstance(result, bool):
                return 'TRUE' if result else 'FALSE'
            elif isinstance(result, float):
                # Format float to avoid scientific notation for small numbers
                # and remove trailing zeros
                if result == int(result):
                    return str(int(result))
                else:
                    # Round to reasonable precision
                    formatted = f"{result:.10f}".rstrip('0').rstrip('.')
                    return formatted
            elif isinstance(result, (list, tuple)):
                # If result is still a list/tuple, join as string
                return ''.join(str(v) if v is not None else '' for v in result)
            else:
                return str(result)
            
        except ZeroDivisionError:
            return '#DIV/0!'
        except NameError as e:
            return '#NAME?'
        except TypeError as e:
            return '#VALUE!'
        except Exception as e:
            return f'#ERROR!'
    
    def add_computed_column(self, column_name: str, values: List[Any], sheet: str = None):
        """Store computed column values with sheet prefix to avoid conflicts."""
        if sheet:
            key = f"{sheet}.{column_name}"
        else:
            key = column_name
        self.computed_columns[key] = values


class ExecutionManager:
    """Orchestrates the execution of formula columns."""
    
    def __init__(self, workbook: WorkbookBuilder):
        self.workbook = workbook
        self.engine = FormulaEngine(workbook)
        self.dep_graph = DependencyGraph()
        self.column_definitions: List[Dict[str, str]] = []
        self.default_sheet = "Sheet1"
    
    def set_default_sheet(self, sheet: str):
        """Set the default sheet for unqualified references."""
        self.default_sheet = sheet
    
    def add_column_definition(self, column_name: str, formula: str, target_sheet: str = None):
        """Add a column definition."""
        target_sheet = target_sheet or self.default_sheet
        
        if not formula.startswith('='):
            formula = '=' + formula
        
        self.column_definitions.append({
            'name': column_name,
            'formula': formula,
            'sheet': target_sheet,
        })
        
        # Get available columns for this sheet
        available_columns = self.workbook.get_all_columns(target_sheet)
        # Add previously defined columns
        for prev_def in self.column_definitions[:-1]:
            available_columns.add(prev_def['name'])
        
        # Extract dependencies
        refs = FormulaParser.extract_column_references(formula, available_columns)
        
        # Build dependency graph
        full_name = f"{target_sheet}.{column_name}"
        self.dep_graph.all_nodes.add(full_name)
        
        for ref in refs:
            ref_full_name = f"{target_sheet}.{ref}"
            self.dep_graph.add_dependency(full_name, ref_full_name)
    
    def validate(self) -> Dict[str, Any]:
        """Validate all column definitions."""
        errors = []
        warnings = []
        
        # Check for circular references
        cycle = self.dep_graph.detect_circular_references()
        if cycle:
            errors.append({
                'type': 'CIRCULAR',
                'message': 'Circular reference detected',
                'details': ' → '.join(cycle),
            })
        
        # Validate column references exist
        for col_def in self.column_definitions:
            available_cols = self.workbook.get_all_columns(col_def['sheet'])
            # Add previously defined formula columns
            for prev_def in self.column_definitions:
                if prev_def['name'] != col_def['name']:
                    available_cols.add(prev_def['name'])
            
            refs = FormulaParser.extract_column_references(col_def['formula'], available_cols)
            for ref in refs:
                if ref not in available_cols:
                    # Check if it's defined in other column definitions
                    defined_cols = {d['name'] for d in self.column_definitions}
                    if ref not in defined_cols:
                        errors.append({
                            'type': 'REF',
                            'message': f"Column '{ref}' not found",
                            'column': col_def['name'],
                        })
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'dependency_order': [f"{d['sheet']}.{d['name']}" for d in self.column_definitions] if not errors else [],
        }
    
    def execute(self) -> Dict[str, Any]:
        """Execute all formula columns."""
        validation = self.validate()
        if not validation['valid']:
            return {'success': False, 'errors': validation['errors']}
        
        results = {}
        
        try:
            for col_def in self.column_definitions:
                sheet = col_def['sheet']
                col_name = col_def['name']
                formula = col_def['formula']
                
                # Evaluate formula
                values = self.engine.evaluate_formula(formula, sheet)
                
                # Store results with sheet prefix to avoid conflicts
                self.engine.add_computed_column(col_name, values, sheet)
                self.workbook.add_column(sheet, col_name, values)
                results[f"{sheet}.{col_name}"] = values
            
            return {'success': True, 'results': results, 'workbook': self.workbook}
            
        except ExcelError as e:
            return {'success': False, 'errors': [ErrorTranslator.translate(e)]}
        except Exception as e:
            return {'success': False, 'errors': [ErrorTranslator.translate(e)]}
    
    def get_result_dataframe(self, sheet: str = None) -> pd.DataFrame:
        """Get the resulting DataFrame with all computed columns."""
        sheet = sheet or self.default_sheet
        return self.workbook.get_sheet_data(sheet)


def create_formula_engine(
    dataframes: Dict[str, pd.DataFrame],
    default_sheet: str = None
) -> ExecutionManager:
    """Factory function to create a configured ExecutionManager."""
    workbook = WorkbookBuilder()
    
    for name, df in dataframes.items():
        workbook.add_sheet(name, df)
    
    manager = ExecutionManager(workbook)
    
    if default_sheet:
        manager.set_default_sheet(default_sheet)
    elif dataframes:
        manager.set_default_sheet(list(dataframes.keys())[0])
    
    return manager
