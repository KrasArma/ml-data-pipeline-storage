
from pydantic import BaseModel
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional





@dataclass
class Config:
    type: str
    id_model: str

    def __post_init__(self):
        if not isinstance(self.type, str):
            raise ValueError("Field 'type' must be a string.")



