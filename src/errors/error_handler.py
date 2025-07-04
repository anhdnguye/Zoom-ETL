import logging
import traceback
import datetime
from errors.error_types import ErrorType
from functools import wraps
import time

def retryable(handler, retries=3, delay=5, error_type=ErrorType.UNKNOWN_ERROR, retry_condition=None):
    """
    Decorator for retrying a function with exponential backoff and error handling.

    Args:
        handler (PipelineErrorHandler): Error handler instance to use.
        retries (int): Number of retry attempts.
        delay (int): Initial delay between retries in seconds.
        error_type (ErrorType): Type of error to log on failure.
        retry_condition (Callable): Optional function to determine if result warrants retry.

    Usage:
        @retryable(handler, retries=3, delay=2, error_type=ErrorType.API_ERROR)
        def fetch_data():
            ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < retries:
                try:
                    result = func(*args, **kwargs)
                    if retry_condition and not retry_condition(result):
                        raise ValueError("Retry condition not satisfied")
                    return result
                except Exception as e:
                    attempt += 1
                    if attempt == retries:
                        handler.handle_error(
                            error_type=error_type,
                            error_message=str(e),
                            context={
                                "function": func.__name__,
                                "args": args,
                                "kwargs": kwargs,
                                "attempt": attempt,
                                "trace": traceback.format_exc()
                            }
                        )
                        raise
                    else:
                        handler.logger.warning(f"[Retry {attempt}/{retries}] {func.__name__} failed: {e}")
                        time.sleep(delay * (2 ** (attempt - 1)))  # Exponential backoff
        return wrapper
    return decorator

class PipelineErrorHandler:
    def __init__(self, admin_email: str, dag_id: str, task_id: str = None):
        """
        Initialize the ErrorHandler with context information.
        
        Args:
            admin_email (str): Email address of the administrator to notify
            dag_id (str): ID of the DAG where the error occurs
            task_id (str, optional): ID of the specific task where error occurs
        """
        self.admin_email = admin_email
        self.dag_id = dag_id
        self.task_id = task_id or {}
        self.logger = logging.getLogger(__name__)
    
    def __call__(self, error_type: str, error_message: str, context: dict = None):
        """
        Make the class callable to handle errors with specific type and message.
        
        Args:
            error_type (str): Type of error (e.g., 'API_ERROR', 'DATABASE_ERROR')
            error_message (str): Detailed error message
            context (dict, optional): Additional context about the error
        """
        return self.handle_error(error_type, error_message, context)
    
    def handle_error(self, error_type: str, error_message: str, context: dict = None):
        """
        Central method to process different types of errors and trigger notifications.
        
        Args:
            error_type (str): Type of error being handled
            error_message (str): Detailed error message
            context (dict, optional): Additional context like user email, API endpoint, etc.
            
        Returns:
            dict: Information about the handled error
        """
        error_details = {
            'dag_id': self.dag_id,
            'task_id': self.task_id,
            'error_type': error_type,
            'error_message': error_message,
            'timestamp': datetime.now().isoformat(),
            'context': context or {},
            'trace': traceback.format_exc()
        }
        
        # Log the error with full context
        self.logger.error(f"Error in DAG {self.dag_id}: {error_details}")
        
        # Process different types of errors
        match error_type:
            case ErrorType.API_ERROR:
                # API_ERROR: Errors related to external API calls failing (e.g., Zoom API)
                # Typically due to rate limiting, authentication issues, or API downtime
                error_details['category'] = 'External Service'
                self._notify_admin(error_details, "API Service Error")
            
            case ErrorType.DATABASE_ERROR:
                # DATABASE_ERROR: Errors related to database connectivity or query execution
                # Could be due to connection timeouts, permission issues, or syntax errors
                error_details['category'] = 'Database'
                self._notify_admin(error_details, "Database Operation Failed")
            
            case ErrorType.AUTHENTICATION_ERROR:
                # AUTHENTICATION_ERROR: Errors related to OAuth token issues or invalid credentials
                # Often requires immediate attention to refresh tokens or update credentials
                error_details['category'] = 'Authentication'
                self._notify_admin(error_details, "Authentication Failure - Immediate Action Required")
            
            case ErrorType.DATA_VALIDATION_ERROR:
                # DATA_VALIDATION_ERROR: Errors related to invalid or unexpected data format
                # Usually indicates issues with API response structure or data processing
                error_details['category'] = 'Data Quality'
                self._notify_admin(error_details, "Data Validation Issue")

            case ErrorType.TIMEOUT_ERROR:
                # TIMEOUT_ERROR: Errors related to long-running API or I/O operations
                # Usually indicates issues with Zoom taking too long
                error_details['category'] = 'Time Out'
                self._notify_admin(error_details, 'Time Out Issue')
            
            case ErrorType.RESOURCE_NOT_FOUND_ERROR:
                # RESOURCE_NOT_FOUND_ERROR: Errors related to resources not found
                # Usually indicates issues with Meeting is deleted or missing, 404 from Zoom
                error_details['category'] = 'Resource Not Found'
                self._notify_admin(error_details, 'Resource Not Found Issue')
                
            case _:
                # Generic error for unclassified issues
                error_details['category'] = 'General'
                self._notify_admin(error_details, "Unexpected Error in DAG")
            
        return error_details

    def _notify_admin(self, error_details: dict, subject_prefix: str):
        """
        Send notification to admin with error details.
        This is a placeholder - implement actual notification logic based on your needs
        (e.g., email via SMTP, Slack webhook, etc.)
        
        Args:
            error_details (dict): Full details of the error
            subject_prefix (str): Prefix for notification subject line
        """
        try:
            notification_message = f"""
            Subject: {subject_prefix} in DAG {self.dag_id}
            
            Error Type: {error_details['error_type']}
            Category: {error_details['category']}
            Message: {error_details['error_message']}
            Timestamp: {error_details['timestamp']}
            DAG ID: {self.dag_id}
            Task ID: {self.task_id or 'N/A'}
            Additional Context: {error_details['context']}
            """
            
            self.logger.info(f"Would send notification to {self.admin_email}: {notification_message}")
            # TODO: Implement actual notification mechanism
            # Example: send_email(self.admin_email, subject, notification_message)
            # Or use Airflow's EmailOperator or SlackWebhookOperator
            
        except Exception as e:
            self.logger.error(f"Failed to send notification to {self.admin_email}: {e}")
