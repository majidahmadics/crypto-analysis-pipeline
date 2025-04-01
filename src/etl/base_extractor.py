"""Base extractor class for the ETL pipeline."""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.utils.config import PROJECT_ROOT

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"{PROJECT_ROOT}/logs/etl.log", mode="a"),
        logging.StreamHandler(),
    ],
)


class BaseExtractor(ABC):
    """Abstract base class for all data extractors.
    
    This class defines the interface that all extractor classes must implement.
    """

    def __init__(self, name: str = "base_extractor"):
        """Initialize the base extractor.
        
        Args:
            name: A name for this extractor instance.
        """
        self.name = name
        self.logger = logging.getLogger(f"extractor.{name}")
        self.logger.info(f"Initializing {name} extractor")
    
    @abstractmethod
    def extract(self, **kwargs) -> Any:
        """Extract data from the source.
        
        Args:
            **kwargs: Additional arguments specific to the extractor implementation.
            
        Returns:
            The extracted data in a format specific to the implementation.
        """
        pass
    
    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validate the extracted data.
        
        Args:
            data: The data to validate.
            
        Returns:
            True if the data is valid, False otherwise.
        """
        pass
    
    def log_extraction_stats(self, data: Any) -> None:
        """Log statistics about the extracted data.
        
        Args:
            data: The extracted data.
        """
        self.logger.info(f"Extraction completed for {self.name}")
        # Subclasses should override this method to provide more detailed statistics