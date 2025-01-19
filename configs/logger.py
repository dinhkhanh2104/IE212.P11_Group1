import logging
import logging.config
import yaml
import os

# Load logging config file
config_folder_path = os.path.dirname(os.path.abspath(__file__))
config_logging_file_path = os.path.join(config_folder_path, "logging_config.yml")

print(config_folder_path)
print(config_logging_file_path)

# Check if config file exits
if not os.path.exists(config_logging_file_path):
    raise FileNotFoundError(f"Config file not found at {config_logging_file_path}")

with open(config_logging_file_path, 'r') as file:
    config = yaml.safe_load(file)
    logging.config.dictConfig(config)

# Create a logger
logger = logging.getLogger('my_logger')

logger.debug("This is a debug message")
logger.info("This is an info message")
logger.warning("This is a warning message")
logger.error("This is an error message")
logger.critical("This is a critical message")