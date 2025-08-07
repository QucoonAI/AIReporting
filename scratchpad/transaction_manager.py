from typing import List, Callable, Any, Dict, Optional
from dataclasses import dataclass
from enum import Enum
import asyncio
from app.core.utils import logger

class OperationStatus(Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    COMPENSATED = "compensated"

@dataclass
class Operation:
    """Represents a single operation in a transaction"""
    name: str
    execute: Callable[[], Any]
    compensate: Optional[Callable[[], Any]] = None
    result: Any = None
    status: OperationStatus = OperationStatus.PENDING
    error: Optional[Exception] = None

class TransactionManager:
    """
    Manages atomic operations across multiple services using compensation pattern
    """
    
    def __init__(self):
        self.operations: List[Operation] = []
        self.completed_operations: List[Operation] = []
    
    def add_operation(
        self, 
        name: str, 
        execute_func: Callable[[], Any], 
        compensate_func: Optional[Callable[[], Any]] = None
    ):
        """Add an operation to the transaction"""
        operation = Operation(
            name=name,
            execute=execute_func,
            compensate=compensate_func
        )
        self.operations.append(operation)
        logger.debug(f"Added operation to transaction: {name}")
    
    async def execute_transaction(self) -> Dict[str, Any]:
        """
        Execute all operations in sequence, with compensation on failure
        """
        try:
            # Execute all operations
            for operation in self.operations:
                try:
                    logger.info(f"Executing operation: {operation.name}")
                    
                    if asyncio.iscoroutinefunction(operation.execute):
                        operation.result = await operation.execute()
                    else:
                        operation.result = operation.execute()
                    
                    operation.status = OperationStatus.SUCCESS
                    self.completed_operations.append(operation)
                    
                    logger.info(f"Operation completed successfully: {operation.name}")
                    
                except Exception as e:
                    operation.error = e
                    operation.status = OperationStatus.FAILED
                    logger.error(f"Operation failed: {operation.name}, error: {e}")
                    
                    # Compensate for all completed operations
                    await self._compensate_operations()
                    
                    return {
                        "success": False,
                        "failed_operation": operation.name,
                        "error": str(e),
                        "compensated_operations": [op.name for op in self.completed_operations if op.status == OperationStatus.COMPENSATED]
                    }
            
            # All operations succeeded
            return {
                "success": True,
                "completed_operations": [op.name for op in self.completed_operations],
                "results": {op.name: op.result for op in self.completed_operations}
            }
            
        except Exception as e:
            logger.error(f"Transaction execution failed: {e}")
            await self._compensate_operations()
            return {
                "success": False,
                "error": str(e),
                "compensated_operations": [op.name for op in self.completed_operations if op.status == OperationStatus.COMPENSATED]
            }
    
    async def _compensate_operations(self):
        """Compensate all completed operations in reverse order"""
        logger.info("Starting compensation for completed operations")
        
        # Reverse order compensation
        for operation in reversed(self.completed_operations):
            if operation.compensate and operation.status == OperationStatus.SUCCESS:
                try:
                    logger.info(f"Compensating operation: {operation.name}")
                    
                    if asyncio.iscoroutinefunction(operation.compensate):
                        await operation.compensate()
                    else:
                        operation.compensate()
                    
                    operation.status = OperationStatus.COMPENSATED
                    logger.info(f"Operation compensated successfully: {operation.name}")
                    
                except Exception as comp_error:
                    logger.error(f"Compensation failed for operation {operation.name}: {comp_error}")
                    # Continue with other compensations even if one fails
    
    def get_transaction_summary(self) -> Dict[str, Any]:
        """Get summary of transaction execution"""
        return {
            "total_operations": len(self.operations),
            "completed_operations": len([op for op in self.operations if op.status == OperationStatus.SUCCESS]),
            "failed_operations": len([op for op in self.operations if op.status == OperationStatus.FAILED]),
            "compensated_operations": len([op for op in self.operations if op.status == OperationStatus.COMPENSATED]),
            "operations_detail": [
                {
                    "name": op.name,
                    "status": op.status.value,
                    "error": str(op.error) if op.error else None
                }
                for op in self.operations
            ]
        }

class DataSourceTransactionManager(TransactionManager):
    """
    Specialized transaction manager for data source operations
    """
    
    def add_s3_upload_operation(self, file_obj, user_id: int, data_source_name: str, s3_service):
        """Add S3 upload operation with compensation"""
        uploaded_url = None
        
        async def upload():
            nonlocal uploaded_url
            from app.core.utils import upload_file_to_s3
            uploaded_url = await upload_file_to_s3(file_obj, user_id, data_source_name)
            return uploaded_url
        
        async def compensate():
            if uploaded_url:
                from app.core.utils import delete_file_from_s3, extract_s3_key_from_url
                try:
                    s3_key = extract_s3_key_from_url(uploaded_url)
                    await delete_file_from_s3(s3_key)
                except Exception as e:
                    logger.warning(f"Failed to cleanup S3 file during compensation: {e}")
        
        self.add_operation("s3_upload", upload, compensate)
    
    def add_database_update_operation(self, repo_method, *args, **kwargs):
        """Add database update operation"""
        async def update():
            return await repo_method(*args, **kwargs)
        
        # Note: Database compensation would require storing previous state
        # For now, we rely on database transactions for atomicity
        self.add_operation("database_update", update)
    
    def add_temp_data_cleanup_operation(self, temp_service, temp_identifier: str, user_id: int):
        """Add temporary data cleanup operation"""
        async def cleanup():
            return await temp_service.cleanup_extraction_and_files(temp_identifier, user_id)
        
        # No compensation needed for cleanup
        self.add_operation("temp_data_cleanup", cleanup)
    
    def add_old_file_cleanup_operation(self, old_s3_url: str):
        """Add old file cleanup operation"""
        async def cleanup():
            from app.core.utils import delete_file_from_s3, extract_s3_key_from_url
            s3_key = extract_s3_key_from_url(old_s3_url)
            await delete_file_from_s3(s3_key)
            return True
        
        # No compensation for old file cleanup (it's already a cleanup operation)
        self.add_operation("old_file_cleanup", cleanup)