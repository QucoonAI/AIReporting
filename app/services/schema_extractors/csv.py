import pandas as pd
import numpy as np
import json
from typing import Dict, Any, Optional, Union
import io
import asyncio

from fastapi import UploadFile

async def extract_csv_schema(
    upload_file: UploadFile, 
    sample_size: Optional[int] = None,
    delimiter: str = ',',
    encoding: str = 'utf-8'
) -> str:
    """
    Extracts schema information from a FastAPI UploadFile object and returns it as JSON.
    
    Args:
        upload_file (UploadFile): FastAPI UploadFile object containing CSV data
        sample_size (int, optional): Number of rows to analyze (None for all rows)
        delimiter (str): CSV delimiter (default: ',')
        encoding (str): Content encoding (default: 'utf-8')
    
    Returns:
        str: JSON string containing the CSV schema information
        
    Raises:
        ValueError: If file is not a CSV or is empty
        Exception: For other pandas reading errors
    """
    
    try:
        # Get file info
        file_name = upload_file.filename or "uploaded_file.csv"
        content_type = getattr(upload_file, 'content_type', 'text/csv')
        
        # Validate content type (optional)
        if content_type and not any(ct in content_type.lower() for ct in ['csv', 'text', 'plain']):
            print(f"Warning: Content type '{content_type}' may not be CSV")
        
        # Read file content
        file_content = await upload_file.read()
        
        # Reset file position for potential future reads
        await upload_file.seek(0)
        
        # Handle both bytes and string content
        if isinstance(file_content, bytes):
            try:
                # Decode bytes to string
                csv_content = file_content.decode(encoding)
            except UnicodeDecodeError:
                # Try common encodings if specified encoding fails
                for fallback_encoding in ['utf-8', 'latin1', 'cp1252']:
                    try:
                        csv_content = file_content.decode(fallback_encoding)
                        encoding = fallback_encoding  # Update encoding for reference
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    raise ValueError(f"Could not decode file with encoding: {encoding}")
        else:
            csv_content = file_content
        
        if not csv_content.strip():
            raise ValueError("File is empty")
        
        # Read CSV content using StringIO
        df = pd.read_csv(
            io.StringIO(csv_content),
            delimiter=delimiter,
            nrows=sample_size,
            low_memory=False
        )
        
        if df.empty:
            raise ValueError("CSV file contains no data")
        
        # Initialize schema structure
        schema_info = {
            "file_info": {
                "file_name": file_name,
            },
            "data_info": {
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "sample_size": sample_size if sample_size else len(df),
            },
            "columns": {},
        }
        
        # Analyze each column
        for column in df.columns:
            col_data = df[column]
            column_info = _analyze_column(col_data)
            schema_info["columns"][column] = column_info
        
        return json.dumps(schema_info, indent=2, default=str)
        
    except Exception as e:
        raise Exception(f"Error processing uploaded file: {e}")

# Synchronous version for non-async contexts
def extract_csv_schema_sync(
    file_content: Union[str, bytes], 
    file_name: str = "uploaded_file.csv",
    sample_size: Optional[int] = None,
    delimiter: str = ',',
    encoding: str = 'utf-8'
) -> str:
    """
    Synchronous version for when you already have the file content.
    
    Args:
        file_content (Union[str, bytes]): File content as string or bytes
        file_name (str): Name of the file for reference
        sample_size (int, optional): Number of rows to analyze (None for all rows)
        delimiter (str): CSV delimiter (default: ',')
        encoding (str): Content encoding (default: 'utf-8')
    
    Returns:
        str: JSON string containing the CSV schema information
    """
    
    try:
        # Handle bytes content
        if isinstance(file_content, bytes):
            try:
                csv_content = file_content.decode(encoding)
            except UnicodeDecodeError:
                # Try common encodings
                for fallback_encoding in ['utf-8', 'latin1', 'cp1252']:
                    try:
                        csv_content = file_content.decode(fallback_encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    raise ValueError(f"Could not decode content with encoding: {encoding}")
        else:
            csv_content = file_content
        
        if not csv_content.strip():
            raise ValueError("Content is empty")
        
        # Read CSV content
        df = pd.read_csv(
            io.StringIO(csv_content),
            delimiter=delimiter,
            nrows=sample_size,
            low_memory=False
        )
        
        if df.empty:
            raise ValueError("CSV content contains no data")
        
        # Initialize schema structure
        schema_info = {
            "file_info": {
                "file_name": file_name,
            },
            "data_info": {
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "sample_size": sample_size if sample_size else len(df),
            },
            "columns": {},
        }
        
        # Analyze each column
        for column in df.columns:
            col_data = df[column]
            column_info = _analyze_column(col_data)
            schema_info["columns"][column] = column_info
        
        return json.dumps(schema_info, indent=2, default=str)
        
    except Exception as e:
        raise Exception(f"Error processing content: {e}")

def _analyze_column(col_data: pd.Series) -> Dict[str, Any]:
    """Analyze individual column characteristics."""
    
    total_count = len(col_data)
    null_count = col_data.isnull().sum()
    non_null_count = total_count - null_count
    
    column_info = {
        "data_type": str(col_data.dtype),
        "null_count": int(null_count),
        "non_null_count": int(non_null_count),
        "null_percentage": round((null_count / total_count) * 100, 2) if total_count > 0 else 0,
        "unique_count": int(col_data.nunique()),
        "unique_percentage": round((col_data.nunique() / total_count) * 100, 2) if total_count > 0 else 0,
        "is_unique": col_data.nunique() == non_null_count and null_count == 0,
    }
    
    # Add basic statistics for non-null values
    if non_null_count > 0:
        non_null_data = col_data.dropna()
        
        # For numeric columns
        if pd.api.types.is_numeric_dtype(col_data):
            column_info.update({
                "min_value": float(non_null_data.min()) if not pd.isna(non_null_data.min()) else None,
                "max_value": float(non_null_data.max()) if not pd.isna(non_null_data.max()) else None,
                "mean": float(non_null_data.mean()) if not pd.isna(non_null_data.mean()) else None,
                "median": float(non_null_data.median()) if not pd.isna(non_null_data.median()) else None,
                "std_dev": float(non_null_data.std()) if not pd.isna(non_null_data.std()) else None,
            })
        
        # For string/object columns
        elif col_data.dtype == 'object':
            str_lengths = non_null_data.astype(str).str.len()
            column_info.update({
                "min_length": int(str_lengths.min()),
                "max_length": int(str_lengths.max()),
                "avg_length": round(str_lengths.mean(), 2),
                "sample_values": list(non_null_data.head(3).astype(str))
            })
        
        # Add inferred type
        column_info["inferred_type"] = _infer_type(non_null_data)
    
    return column_info

def _infer_type(series: pd.Series) -> str:
    """Infer more specific data types."""
    
    # Convert to string for pattern matching
    str_series = series.astype(str)
    
    # Sample first 100 values for pattern detection
    sample = str_series.head(100)
    sample_size = len(sample)
    
    if sample_size == 0:
        return "unknown"
    
    # Date patterns
    date_patterns = [
        r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
        r'^\d{2}/\d{2}/\d{4}$',  # MM/DD/YYYY
        r'^\d{2}-\d{2}-\d{4}$',  # MM-DD-YYYY
    ]
    
    # DateTime patterns
    datetime_patterns = [
        r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',  # YYYY-MM-DD HH:MM:SS
        r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # ISO format
    ]
    
    # Email pattern
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    # Phone pattern
    phone_pattern = r'^[\+]?[1-9]?[\d\s\-\(\)]{10,}$'
    
    # URL pattern
    url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
    
    # Check patterns (80% threshold for pattern match)
    if any(sample.str.match(pattern).sum() / sample_size > 0.8 for pattern in datetime_patterns):
        return "datetime"
    elif any(sample.str.match(pattern).sum() / sample_size > 0.8 for pattern in date_patterns):
        return "date"
    elif sample.str.match(email_pattern).sum() / sample_size > 0.8:
        return "email"
    elif sample.str.match(phone_pattern).sum() / sample_size > 0.8:
        return "phone"
    elif sample.str.match(url_pattern).sum() / sample_size > 0.8:
        return "url"
    elif pd.api.types.is_numeric_dtype(series):
        if series.dtype in ['int64', 'int32', 'int16', 'int8']:
            return "integer"
        elif series.dtype in ['float64', 'float32']:
            # Check if it's actually integer values stored as float
            if (series % 1 == 0).all():
                return "integer_as_float"
            return "decimal"
    elif series.dtype == 'bool':
        return "boolean"
    elif series.dtype == 'object':
        # Check if it's categorical-like (low unique ratio)
        if series.nunique() / len(series) < 0.1:
            return "categorical"
        return "text"
    
    return str(series.dtype)

# Example usage
if __name__ == "__main__":
    # Example CSV content for testing
    csv_content = """id,name,email,age,salary,department,active
1,John Doe,john@example.com,25,50000,Engineering,true
2,Jane Smith,jane@example.com,30,60000,Marketing,true
3,Bob Johnson,bob@example.com,35,55000,Sales,false
4,Alice Brown,alice@example.com,28,52000,Engineering,true
5,Charlie Wilson,charlie@example.com,32,58000,Marketing,true"""
    
    print("=== Testing Synchronous Version ===")
    try:
        # Test with string content
        schema_json = extract_csv_schema_sync(csv_content, "employees.csv")
        print("Schema extracted from string content:")
        result = json.loads(schema_json)
        print(f"File: {result['file_info']['file_name']}")
        print(f"Rows: {result['data_info']['total_rows']}, Columns: {result['data_info']['total_columns']}")
        print(f"Sample columns: {list(result['columns'].keys())[:3]}")
        
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n=== Testing with Bytes Content ===")
    try:
        # Test with bytes content
        csv_bytes = csv_content.encode('utf-8')
        schema_json = extract_csv_schema_sync(csv_bytes, "employees_bytes.csv")
        result = json.loads(schema_json)
        print(f"Schema extracted from bytes content:")
        print(f"File: {result['file_info']['file_name']}")
        print(f"Rows: {result['data_info']['total_rows']}")
        
    except Exception as e:
        print(f"Error: {e}")
    
    # Mock FastAPI UploadFile for testing async version
    print("\n=== Mock FastAPI UploadFile Test ===")
    
    class MockUploadFile:
        """Mock UploadFile for testing purposes"""
        def __init__(self, content: bytes, filename: str, content_type: str = "text/csv"):
            self.content = content
            self.filename = filename
            self.content_type = content_type
            self._position = 0
        
        async def read(self) -> bytes:
            return self.content
        
        async def seek(self, position: int = 0) -> None:
            self._position = position
    
    async def test_async_version():
        try:
            # Create mock upload file
            mock_file = MockUploadFile(
                content=csv_content.encode('utf-8'),
                filename="uploaded_employees.csv",
                content_type="text/csv"
            )
            
            # Test async extraction
            schema_json = await extract_csv_schema(mock_file)
            result = json.loads(schema_json)
            print("Schema extracted from mock UploadFile:")
            print(f"File: {result['file_info']['file_name']}")
            print(f"Rows: {result['data_info']['total_rows']}, Columns: {result['data_info']['total_columns']}")
            
            # Test with sample size
            schema_json_sample = await extract_csv_schema(mock_file, sample_size=3)
            result_sample = json.loads(schema_json_sample)
            print(f"With sample size 3: {result_sample['data_info']['sample_size']} rows analyzed")
            
        except Exception as e:
            print(f"Error in async test: {e}")
    
    # Run async test
    asyncio.run(test_async_version())
    
    print("\n=== FastAPI Integration Example ===")
    print("""
# FastAPI endpoint example:

from fastapi import FastAPI, UploadFile, File, HTTPException
import json

app = FastAPI()

@app.post("/analyze-csv/")
async def analyze_csv_file(file: UploadFile = File(...)):
    try:
        # Validate file type
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Only CSV files are allowed")
        
        # Extract schema
        schema_json = await extract_csv_schema(file)
        schema_data = json.loads(schema_json)
        
        return {
            "status": "success",
            "message": "CSV schema extracted successfully",
            "schema": schema_data
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

@app.post("/analyze-csv-sample/")
async def analyze_csv_sample(file: UploadFile = File(...), sample_size: int = 1000):
    try:
        schema_json = await extract_csv_schema(file, sample_size=sample_size)
        schema_data = json.loads(schema_json)
        
        return {
            "status": "success",
            "schema": schema_data,
            "note": f"Analysis based on first {sample_size} rows"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    """)
