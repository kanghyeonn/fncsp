from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any
from datetime import datetime
import traceback

class BaseRepository(ABC):

    def __init__(self):
        self.connection = None
        self._is_connected = False

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def is_connected(self):
        pass

