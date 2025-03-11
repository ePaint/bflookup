import datetime
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel


class Settings(BaseModel):
    MOVE_FILES_TO_PROCESSED_FOLDER: Optional[bool] = False
    ADD_TIMESTAMP_TO_PROCESSED_FILES: Optional[bool] = False
    DATA_FOLDER: Optional[str] = "data"
    LOOKUP_FOLDER: Optional[str] = "lookup"
    OUTPUT_FOLDER: Optional[str] = "output"
    FORCE_UPC_EXCEL_STRING: Optional[bool] = False
    TIMESTAMP: Optional[str] = ""


file_path = Path("settings.yml")
if file_path.exists():
    yml_dict = yaml.safe_load(file_path.read_text())
    yml_dict["TIMESTAMP"] = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    SETTINGS = Settings(**yml_dict)
