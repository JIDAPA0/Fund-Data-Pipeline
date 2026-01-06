import hashlib
import json
from typing import Dict, Any

# =======================================================
# 1. ฟังก์ชันที่ Scraper เรียกใช้ (รับ Dictionary)
# =======================================================
def generate_row_hash(row_data: Dict[str, Any]) -> str:
    """
    สร้าง Hash จาก Dictionary โดยเรียง Key อัตโนมัติเพื่อให้ค่า Hash คงที่
    (Scraper 01_ft_identity.py เรียกใช้ตัวนี้)
    """
    
    encoded_str = json.dumps(row_data, sort_keys=True, default=str).encode('utf-8')
    return hashlib.md5(encoded_str).hexdigest()

# =======================================================
# 2. ฟังก์ชันเดิมของพี่ (รับค่าหลายตัว)
# =======================================================
def calculate_row_hash(*args):
    """
    สร้าง Hash (MD5) จากข้อมูลที่ส่งเข้ามา (Arguments)
    calculate_row_hash(val1, val2, val3)
    """
    concatenated_string = "".join(str(arg) if arg is not None else "" for arg in args)
    return hashlib.md5(concatenated_string.encode('utf-8')).hexdigest()