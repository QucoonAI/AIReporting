import json
import asyncio
from typing import Dict, List, Any, Optional
import io
from datetime import datetime, date, time
import re
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.cell.cell import TYPE_STRING, TYPE_NUMERIC, TYPE_BOOL, TYPE_FORMULA, TYPE_ERROR, TYPE_NULL
from fastapi import UploadFile


async def extract_xlsx_schema(
    upload_file: UploadFile, 
    sample_size: Optional[int] = None,
    analyze_all_sheets: bool = True,
    sheet_name: Optional[str] = None
) -> str:
    """
    Extracts schema information from a FastAPI UploadFile object containing an XLSX file.
    
    Args:
        upload_file (UploadFile): FastAPI UploadFile object containing XLSX data
        sample_size (int, optional): Number of rows to analyze per sheet (None for all rows)
        analyze_all_sheets (bool): Whether to analyze all sheets or just the first one
        sheet_name (str, optional): Specific sheet name to analyze (overrides analyze_all_sheets)
    
    Returns:
        str: JSON string containing the XLSX schema information
        
    Raises:
        ValueError: If file is not an XLSX file or is empty/corrupted
        Exception: For other processing errors
    """
    
    try:
        # Get file info
        file_name = upload_file.filename or "uploaded_file.xlsx"
        content_type = getattr(upload_file, 'content_type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        
        # Validate file extension
        if not file_name.lower().endswith(('.xlsx', '.xlsm')):
            raise ValueError("File must be an Excel file (.xlsx or .xlsm)")
        
        # Read file content
        file_content = await upload_file.read()
        
        # Reset file position for potential future reads
        await upload_file.seek(0)
        
        if not file_content:
            raise ValueError("File is empty")
        
        # Load workbook from bytes
        workbook = load_workbook(io.BytesIO(file_content), data_only=True, read_only=True)
        
        # Initialize schema structure
        schema_info = {
            "file_info": {
                "file_name": file_name,
                "content_type": content_type,
                "file_size_bytes": len(file_content),
                "file_size_mb": round(len(file_content) / (1024 * 1024), 2)
            },
            "workbook_info": {
                "total_sheets": len(workbook.sheetnames),
                "sheet_names": workbook.sheetnames,
                "active_sheet": workbook.active.title if workbook.active else None
            },
            "sheets": {}
        }
        
        # Determine which sheets to analyze
        sheets_to_analyze = []
        if sheet_name:
            if sheet_name in workbook.sheetnames:
                sheets_to_analyze = [sheet_name]
            else:
                raise ValueError(f"Sheet '{sheet_name}' not found. Available sheets: {workbook.sheetnames}")
        elif analyze_all_sheets:
            sheets_to_analyze = workbook.sheetnames
        else:
            sheets_to_analyze = [workbook.sheetnames[0]] if workbook.sheetnames else []
        
        # Analyze each sheet
        for sheet_name in sheets_to_analyze:
            sheet = workbook[sheet_name]
            sheet_info = _analyze_sheet(sheet, sample_size)
            schema_info["sheets"][sheet_name] = sheet_info
        
        workbook.close()
        
        return json.dumps(schema_info, indent=2, default=str)
        
    except Exception as e:
        raise Exception(f"Error processing XLSX file: {e}")

def _analyze_sheet(sheet, sample_size: Optional[int] = None) -> Dict[str, Any]:
    """Analyze individual sheet characteristics."""
    
    # Get sheet dimensions
    max_row = sheet.max_row
    max_col = sheet.max_column
    
    # Determine actual data range (excluding empty rows/columns)
    actual_max_row, actual_max_col = _get_actual_data_range(sheet)
    
    # Limit rows if sample_size is specified
    rows_to_analyze = min(sample_size, actual_max_row) if sample_size else actual_max_row
    
    sheet_info = {
        "sheet_info": {
            "max_row": max_row,
            "max_column": max_col,
            "actual_max_row": actual_max_row,
            "actual_max_column": actual_max_col,
            "rows_analyzed": rows_to_analyze,
            "has_merged_cells": len(sheet.merged_cells.ranges) > 0,
            "merged_cell_count": len(sheet.merged_cells.ranges)
        },
        "data_info": {
            "total_rows": actual_max_row,
            "total_columns": actual_max_col,
            "sample_size": sample_size if sample_size else actual_max_row,
            "empty_rows": 0,
            "empty_columns": 0
        },
        "columns": {}
    }
    
    if actual_max_row == 0 or actual_max_col == 0:
        return sheet_info
    
    # Detect headers (assume first row contains headers)
    headers = []
    for col in range(1, actual_max_col + 1):
        cell = sheet.cell(row=1, column=col)
        header = str(cell.value) if cell.value is not None else f"Column_{get_column_letter(col)}"
        headers.append(header)
    
    # Analyze each column
    for col_idx, header in enumerate(headers, 1):
        column_letter = get_column_letter(col_idx)
        column_data = []
        
        # Collect data from column (starting from row 2 to skip header)
        start_row = 2 if actual_max_row > 1 else 1
        end_row = min(start_row + rows_to_analyze - 1, actual_max_row) if sample_size else actual_max_row
        
        for row in range(start_row, end_row + 1):
            cell = sheet.cell(row=row, column=col_idx)
            column_data.append(cell)
        
        # Analyze column
        column_info = _analyze_excel_column(column_data, header, column_letter)
        sheet_info["columns"][header] = column_info
    
    # Calculate empty rows and columns
    sheet_info["data_info"]["empty_rows"] = _count_empty_rows(sheet, actual_max_row, actual_max_col)
    sheet_info["data_info"]["empty_columns"] = _count_empty_columns(sheet, actual_max_row, actual_max_col)
    
    return sheet_info

def _get_actual_data_range(sheet) -> tuple:
    """Get the actual data range excluding empty rows and columns."""
    max_row = 0
    max_col = 0
    
    for row in sheet.iter_rows():
        row_num = row[0].row
        has_data = any(cell.value is not None for cell in row)
        if has_data:
            max_row = max(max_row, row_num)
            # Find the rightmost column with data in this row
            for col_idx, cell in enumerate(row, 1):
                if cell.value is not None:
                    max_col = max(max_col, col_idx)
    
    return max_row, max_col

def _count_empty_rows(sheet, max_row: int, max_col: int) -> int:
    """Count completely empty rows."""
    empty_count = 0
    for row in range(1, max_row + 1):
        is_empty = all(sheet.cell(row=row, column=col).value is None 
                      for col in range(1, max_col + 1))
        if is_empty:
            empty_count += 1
    return empty_count

def _count_empty_columns(sheet, max_row: int, max_col: int) -> int:
    """Count completely empty columns."""
    empty_count = 0
    for col in range(1, max_col + 1):
        is_empty = all(sheet.cell(row=row, column=col).value is None 
                      for row in range(1, max_row + 1))
        if is_empty:
            empty_count += 1
    return empty_count

def _analyze_excel_column(column_data: List, header: str, column_letter: str) -> Dict[str, Any]:
    """Analyze individual Excel column characteristics."""
    
    total_count = len(column_data)
    null_count = sum(1 for cell in column_data if cell.value is None)
    non_null_count = total_count - null_count
    
    # Get non-null values and cells
    non_null_cells = [cell for cell in column_data if cell.value is not None]
    non_null_values = [cell.value for cell in non_null_cells]
    
    column_info = {
        "column_letter": column_letter,
        "header": header,
        "total_count": total_count,
        "null_count": null_count,
        "non_null_count": non_null_count,
        "null_percentage": round((null_count / total_count) * 100, 2) if total_count > 0 else 0,
        "unique_count": len(set(str(v) for v in non_null_values)) if non_null_values else 0,
        "unique_percentage": 0,
        "is_unique": False
    }
    
    if non_null_count > 0:
        unique_count = len(set(str(v) for v in non_null_values))
        column_info["unique_count"] = unique_count
        column_info["unique_percentage"] = round((unique_count / total_count) * 100, 2)
        column_info["is_unique"] = unique_count == non_null_count and null_count == 0
        
        # Analyze data types
        type_analysis = _analyze_excel_types(non_null_cells)
        column_info.update(type_analysis)
        
        # Get sample values
        column_info["sample_values"] = [str(v) for v in non_null_values[:5]]
        
        # Statistical analysis for numeric data
        if type_analysis.get("primary_type") in ["numeric", "integer", "decimal"]:
            numeric_values = [float(v) for v in non_null_values if isinstance(v, (int, float))]
            if numeric_values:
                column_info.update({
                    "min_value": min(numeric_values),
                    "max_value": max(numeric_values),
                    "mean": sum(numeric_values) / len(numeric_values),
                    "median": sorted(numeric_values)[len(numeric_values)//2]
                })
        
        # String analysis for text data
        elif type_analysis.get("primary_type") == "text":
            str_lengths = [len(str(v)) for v in non_null_values]
            if str_lengths:
                column_info.update({
                    "min_length": min(str_lengths),
                    "max_length": max(str_lengths),
                    "avg_length": round(sum(str_lengths) / len(str_lengths), 2)
                })
        
        # Pattern analysis
        column_info["patterns"] = _analyze_excel_patterns(non_null_values)
    
    return column_info

def _analyze_excel_types(cells: List) -> Dict[str, Any]:
    """Analyze the data types in Excel cells."""
    
    type_counts = {
        "numeric": 0,
        "text": 0,
        "boolean": 0,
        "date": 0,
        "datetime": 0,
        "time": 0,
        "formula": 0,
        "error": 0
    }
    
    values = []
    
    for cell in cells:
        value = cell.value
        values.append(value)
        
        # Check Excel cell type first
        if hasattr(cell, 'data_type'):
            if cell.data_type == TYPE_NUMERIC:
                # Could be number, date, or time
                if isinstance(value, datetime):
                    type_counts["datetime"] += 1
                elif isinstance(value, date):
                    type_counts["date"] += 1
                elif isinstance(value, time):
                    type_counts["time"] += 1
                else:
                    type_counts["numeric"] += 1
            elif cell.data_type == TYPE_STRING:
                type_counts["text"] += 1
            elif cell.data_type == TYPE_BOOL:
                type_counts["boolean"] += 1
            elif cell.data_type == TYPE_FORMULA:
                type_counts["formula"] += 1
            elif cell.data_type == TYPE_ERROR:
                type_counts["error"] += 1
        else:
            # Fallback type detection
            if isinstance(value, bool):
                type_counts["boolean"] += 1
            elif isinstance(value, (int, float)):
                type_counts["numeric"] += 1
            elif isinstance(value, datetime):
                type_counts["datetime"] += 1
            elif isinstance(value, date):
                type_counts["date"] += 1
            elif isinstance(value, time):
                type_counts["time"] += 1
            else:
                type_counts["text"] += 1
    
    # Determine primary type
    primary_type = max(type_counts, key=type_counts.get)
    total_cells = len(cells)
    
    # More specific numeric analysis
    if primary_type == "numeric":
        numeric_values = [v for v in values if isinstance(v, (int, float))]
        if numeric_values:
            all_integers = all(isinstance(v, int) or (isinstance(v, float) and v.is_integer()) 
                             for v in numeric_values)
            primary_type = "integer" if all_integers else "decimal"
    
    type_distribution = {k: round((v / total_cells) * 100, 2) for k, v in type_counts.items()}
    
    return {
        "primary_type": primary_type,
        "type_distribution": type_distribution,
        "inferred_excel_type": _infer_excel_format(values)
    }

def _infer_excel_format(values: List) -> str:
    """Infer the Excel format based on the values."""
    
    if not values:
        return "unknown"
    
    sample_values = [str(v) for v in values[:100] if v is not None]
    
    # Date patterns
    date_patterns = [
        r'^\d{4}-\d{2}-\d{2}$',
        r'^\d{2}/\d{2}/\d{4}$',
        r'^\d{2}-\d{2}-\d{4}$',
    ]
    
    # Email pattern
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    # Phone pattern
    phone_pattern = r'^[\+]?[1-9]?[\d\s\-\(\)]{10,}$'
    
    # Currency pattern
    currency_pattern = r'^[\$£€¥]?[\d,]+\.?\d*$'
    
    # Percentage pattern
    percentage_pattern = r'^\d+\.?\d*%$'
    
    if not sample_values:
        return "empty"
    
    # Check patterns
    for pattern, name in [
        (email_pattern, "email"),
        (phone_pattern, "phone"),
        (currency_pattern, "currency"),
        (percentage_pattern, "percentage")
    ]:
        matches = sum(1 for v in sample_values if re.match(pattern, str(v)))
        if matches / len(sample_values) > 0.8:
            return name
    
    # Check date patterns
    for pattern in date_patterns:
        matches = sum(1 for v in sample_values if re.match(pattern, str(v)))
        if matches / len(sample_values) > 0.8:
            return "date_string"
    
    # Check if values are mostly numbers
    numeric_count = sum(1 for v in values if isinstance(v, (int, float)))
    if numeric_count / len(values) > 0.8:
        return "number"
    
    # Check if it's categorical (low unique ratio)
    unique_ratio = len(set(sample_values)) / len(sample_values)
    if unique_ratio < 0.1:
        return "categorical"
    
    return "text"

def _analyze_excel_patterns(values: List) -> Dict[str, Any]:
    """Analyze patterns in Excel data."""
    
    if not values:
        return {}
    
    str_values = [str(v) for v in values if v is not None]
    
    patterns = {
        "most_common_values": [],
        "length_distribution": {},
        "character_patterns": {}
    }
    
    # Most common values
    from collections import Counter
    value_counts = Counter(str_values)
    patterns["most_common_values"] = [
        {"value": value, "count": count, "percentage": round((count / len(str_values)) * 100, 2)}
        for value, count in value_counts.most_common(5)
    ]
    
    # Length distribution
    if str_values:
        lengths = [len(v) for v in str_values]
        patterns["length_distribution"] = {
            "min": min(lengths),
            "max": max(lengths),
            "avg": round(sum(lengths) / len(lengths), 2),
            "most_common": Counter(lengths).most_common(1)[0][0]
        }
    
    return patterns

# Synchronous version for non-async contexts
def extract_xlsx_schema_sync(
    file_content: bytes, 
    file_name: str = "uploaded_file.xlsx",
    sample_size: Optional[int] = None,
    analyze_all_sheets: bool = True,
    sheet_name: Optional[str] = None
) -> str:
    """
    Synchronous version for when you already have the file content.
    
    Args:
        file_content (bytes): XLSX file content as bytes
        file_name (str): Name of the file for reference
        sample_size (int, optional): Number of rows to analyze per sheet
        analyze_all_sheets (bool): Whether to analyze all sheets
        sheet_name (str, optional): Specific sheet name to analyze
    
    Returns:
        str: JSON string containing the XLSX schema information
    """
    
    try:
        if not file_content:
            raise ValueError("File content is empty")
        
        # Load workbook from bytes
        workbook = load_workbook(io.BytesIO(file_content), data_only=True, read_only=True)
        
        # Initialize schema structure
        schema_info = {
            "file_info": {
                "file_name": file_name,
                "file_size_bytes": len(file_content),
                "file_size_mb": round(len(file_content) / (1024 * 1024), 2)
            },
            "workbook_info": {
                "total_sheets": len(workbook.sheetnames),
                "sheet_names": workbook.sheetnames,
                "active_sheet": workbook.active.title if workbook.active else None
            },
            "sheets": {}
        }
        
        # Determine which sheets to analyze
        sheets_to_analyze = []
        if sheet_name:
            if sheet_name in workbook.sheetnames:
                sheets_to_analyze = [sheet_name]
            else:
                raise ValueError(f"Sheet '{sheet_name}' not found. Available sheets: {workbook.sheetnames}")
        elif analyze_all_sheets:
            sheets_to_analyze = workbook.sheetnames
        else:
            sheets_to_analyze = [workbook.sheetnames[0]] if workbook.sheetnames else []
        
        # Analyze each sheet
        for sheet_name in sheets_to_analyze:
            sheet = workbook[sheet_name]
            sheet_info = _analyze_sheet(sheet, sample_size)
            schema_info["sheets"][sheet_name] = sheet_info
        
        workbook.close()
        
        return json.dumps(schema_info, indent=2, default=str)
        
    except Exception as e:
        raise Exception(f"Error processing XLSX content: {e}")

# Example usage
if __name__ == "__main__":
    print("=== XLSX Schema Extractor Example ===")
    print("This module is designed to work with FastAPI UploadFile objects.")
    print("\nExample FastAPI usage:")
    print("""
from fastapi import FastAPI, UploadFile, File, HTTPException
import json

app = FastAPI()

@app.post("/analyze-xlsx/")
async def analyze_xlsx_file(file: UploadFile = File(...)):
    try:
        if not file.filename.endswith(('.xlsx', '.xlsm')):
            raise HTTPException(status_code=400, detail="Only Excel files (.xlsx, .xlsm) are allowed")
        
        schema_json = await extract_xlsx_schema(file)
        schema_data = json.loads(schema_json)
        
        return {
            "status": "success",
            "message": "XLSX schema extracted successfully",
            "schema": schema_data
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

@app.post("/analyze-xlsx-sheet/")
async def analyze_specific_sheet(
    file: UploadFile = File(...), 
    sheet_name: str = "Sheet1",
    sample_size: int = 1000
):
    try:
        schema_json = await extract_xlsx_schema(
            file, 
            sample_size=sample_size,
            sheet_name=sheet_name
        )
        return json.loads(schema_json)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    """)
    
    # Mock test
    print("\n=== Mock Test ===")
    
    class MockUploadFile:
        def __init__(self, content: bytes, filename: str):
            self.content = content
            self.filename = filename
            self.content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        
        async def read(self) -> bytes:
            return self.content
        
        async def seek(self, position: int = 0) -> None:
            pass
    
    async def test_mock():
        try:
            # You would need actual XLSX content for a real test
            print("Mock test would require actual XLSX file content")
            print("In a real scenario, you would:")
            print("1. Upload an XLSX file via FastAPI")
            print("2. Call await extract_xlsx_schema(upload_file)")
            print("3. Get detailed schema information for all sheets")
            
        except Exception as e:
            print(f"Mock test error: {e}")
    
    asyncio.run(test_mock())