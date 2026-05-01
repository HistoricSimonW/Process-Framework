from pydantic import BaseModel

class CrudArgs(BaseModel):
    """ creations, updates, deletions """
    creations:bool
    updates:bool
    deletions:bool
    