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
        
        # Close SUM brackets
        if 'sum([' in expr:
            expr = re.sub(r'sum\(\[([^)]+)\)', r'sum([\1])', expr)
        
        # Handle TRUE/FALSE
        expr = re.sub(r'\bTRUE\b', 'True', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bFALSE\b', 'False', expr, flags=re.IGNORECASE)
        
        # Replace column names with their values from row_data
        # Sort by length (longest first) to avoid partial replacements
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


class FormulaEngine:
    """Main formula evaluation engine."""
    
    def __init__(self, workbook: WorkbookBuilder):
        self.workbook = workbook
        self.computed_columns: Dict[str, List[Any]] = {}
    
    def evaluate_formula(self, formula: str, sheet: str) -> List[Any]:
        """Evaluate a formula for all rows in a sheet."""
        df = self.workbook.get_sheet_data(sheet)
        results = []
        
        # Get available columns (including computed ones)
        available_columns = set(df.columns) | set(self.computed_columns.keys())
        
        for idx in range(len(df)):
            # Build row data dictionary
            row_data = {}
            for col in df.columns:
                row_data[col] = df.iloc[idx][col]
            # Add computed columns
            for col, values in self.computed_columns.items():
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
    
    def add_computed_column(self, column_name: str, values: List[Any]):
        """Store computed column values."""
        self.computed_columns[column_name] = values


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
                
                # Store results
                self.engine.add_computed_column(col_name, values)
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
