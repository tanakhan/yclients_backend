import os
from pprint import pprint
import sys, os, csv
import logging
from pathlib import Path

def save_dict_line(file_name, item):
    fields = item.keys()
    file_exists = os.path.isfile(file_name)
    with open(file_name, 'a', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields, delimiter=';')
        if not file_exists:
            writer.writeheader()  # file doesn't exist yet, write a header
        writer.writerow(item)

def create_log_directory(log_dir_type="script"):
    """Create and return the log directory path"""
    if log_dir_type == "home":
        log_dir_path = Path.home() / "logs"
    elif log_dir_type == "same" or log_dir_type == "script":
        log_dir_path = Path(__file__).parent / "logs"
    else:
        raise ValueError("Invalid log_dir_type. Choose 'home', 'same', or 'script'.")
    
    log_dir_path.mkdir(parents=True, exist_ok=True)
    return log_dir_path

def configure_root_logger(base_name, console_level="INFO", file_level="DEBUG", log_dir_type="script"):
    """
    Configure the root logger for the application
    
    Args:
        base_name: Base name for the application logger
        console_level: Logging level for console output
        file_level: Logging level for file output
        log_dir_type: Type of log directory ('home', 'same', or 'script')
    
    Returns:
        tuple: (logger, log_dir_path)
    """
    log_dir_path = create_log_directory(log_dir_type)
    
    # Create the root application logger
    root_logger = logging.getLogger(base_name)
    root_logger.setLevel(logging.DEBUG)
    
    # Clear existing handlers to prevent duplicates on reloads
    if root_logger.handlers:
        root_logger.handlers = []
    
    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(_parse_log_level(console_level))
    console_formatter = logging.Formatter('%(levelname)s - %(name)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # Add file handler for all logs
    all_logs_path = log_dir_path / f"{base_name}_all.log"
    file_handler = logging.FileHandler(all_logs_path)
    file_handler.setLevel(_parse_log_level(file_level))
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s [in %(filename)s:%(lineno)d]')
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)
    
    # Add file handler for errors
    error_logs_path = log_dir_path / f"{base_name}_errors.log"
    error_handler = logging.FileHandler(error_logs_path)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)
    root_logger.addHandler(error_handler)
    
    return root_logger, log_dir_path

def get_module_logger(base_name, module_name, log_dir_path=None, file_level="DEBUG", with_file=True):
    """
    Get or create a module-specific logger
    
    Args:
        base_name: Base name for the application logger
        module_name: Name of the module
        log_dir_path: Directory to store log files
        file_level: Logging level for file output
        with_file: Whether to add a module-specific file handler
    
    Returns:
        logging.Logger: The configured logger
    """
    # Create the logger with the hierarchical name
    logger_name = f"{base_name}.{module_name}"
    logger = logging.getLogger(logger_name)
    
    # If a file handler is requested and log_dir_path is provided
    if with_file and log_dir_path:
        # Check if this logger already has a file handler
        has_file_handler = any(isinstance(h, logging.FileHandler) for h in logger.handlers)
        
        if not has_file_handler:
            # Add a module-specific file handler
            module_log_path = log_dir_path / f"{module_name}.log"
            file_handler = logging.FileHandler(module_log_path)
            file_handler.setLevel(_parse_log_level(file_level))
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s [in %(filename)s:%(lineno)d]')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
    
    return logger

def _parse_log_level(level):
    """Convert string log level to logging level constant."""
    if isinstance(level, int):
        return level
    
    level_map = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "NOTSET": logging.NOTSET
    }
    
    upper_level = level.upper()
    if upper_level in level_map:
        return level_map[upper_level]
    else:
        raise ValueError(f"Invalid log level: {level}")

# Backward compatibility function
def setup_logger(log_file_name, logger_name, console_level, file_level, log_dir_type="script"):
    """
    Legacy function for backward compatibility
    
    This creates a standalone logger with its own handlers
    """
    log_dir_path = create_log_directory(log_dir_type)
    log_path = log_dir_path / log_file_name
    error_log_path = log_dir_path / f"errors_{log_file_name}"
    
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    
    # Clear existing handlers
    if logger.handlers:
        logger.handlers = []
    
    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s [in %(filename)s:%(lineno)d]')
    
    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(_parse_log_level(console_level))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File Handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(_parse_log_level(file_level))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Error File Handler
    error_file_handler = logging.FileHandler(error_log_path)
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(formatter)
    logger.addHandler(error_file_handler)
    
    # Enable propagation by default
    logger.propagate = True
    
    return logger, log_dir_path