from pydantic import BaseModel

class RunMetadata(BaseModel):
    process:str
    process_version:str
    process_model_version:str