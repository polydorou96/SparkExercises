import logging


def setup_logger(log_file_name: str = "SparkInterview.log"):
    # Step 1: Configure the logger
    logging.basicConfig(
        level=logging.INFO,  # Set the logging level
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Define the format of the log messages
        datefmt='%Y-%m-%d %H:%M:%S',  # Date format in the logs
        handlers=[
            logging.FileHandler(log_file_name),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)

    return logger