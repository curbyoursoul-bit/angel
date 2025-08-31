import os

TRUE = {"1","true","yes","on","y","t"}
FALSE = {"0","false","no","off","n","f"}

def get_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip().lower()
    if v in TRUE: return True
    if v in FALSE: return False
    return default
