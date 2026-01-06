import hashlib
import json
from typing import Dict, Any

# =======================================================

# =======================================================
def generate_row_hash(row_data: Dict[str, Any]) -> str:
    
    encoded_str = json.dumps(row_data, sort_keys=True, default=str).encode('utf-8')
    return hashlib.md5(encoded_str).hexdigest()

# =======================================================

# =======================================================
def calculate_row_hash(*args):
    concatenated_string = "".join(str(arg) if arg is not None else "" for arg in args)
    return hashlib.md5(concatenated_string.encode('utf-8')).hexdigest()