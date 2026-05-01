from pydantic import BaseModel

class EtlOpsArgs(BaseModel):
    creations:bool
    updates:bool
    deletions:bool
    