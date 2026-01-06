import sys
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# ==========================================
# CONSTANTS & CONFIGURATION
# ==========================================

# สถานะที่เป็นไปได้
STATUS_NEW = "new"
STATUS_ACTIVE = "active"
STATUS_INACTIVE = "inactive"

# เกณฑ์ระยะเวลา (Grace Period)
# 7 วันตามที่คุณต้องการ เพื่อรองรับวันหยุดและ Server Down ชั่วคราว
INACTIVE_THRESHOLD_DAYS = 7 

class StatusManager:
    """
    คลาสกลางสำหรับจัดการ Logic การเปลี่ยนสถานะของกองทุน
    """

    @staticmethod
    def get_inactive_cutoff_date(reference_date: datetime = None) -> str:
        """
        คืนค่าวันที่ 'เส้นตาย' สำหรับการตัดเป็น Inactive
        Logic: วันที่ปัจจุบัน - 7 วัน
        Return: String 'YYYY-MM-DD'
        """
        if reference_date is None:
            reference_date = datetime.now()
        
        cutoff_date = reference_date - timedelta(days=INACTIVE_THRESHOLD_DAYS)
        return cutoff_date.strftime("%Y-%m-%d")

    @staticmethod
    def determine_initial_status(ticker: str, name: str, source: str) -> str:
        """
        กำหนดสถานะเริ่มต้นสำหรับข้อมูลที่เพิ่งเข้ามาใหม่ (Ingested)
        """
        # ถ้าไม่มีชื่อ (Name) หรือข้อมูลสำคัญหายไป ให้เป็น new ไว้ก่อนเพื่อรอตรวจสอบ
        if not name or name.strip() == "" or name.lower() == "nan":
            return STATUS_NEW
        
        # กรณีปกติ ถ้าข้อมูลครบถ้วน อาจจะให้เป็น new ไว้ก่อน 
        # เพื่อรอผ่าน Validator ในขั้นตอนถัดไป หรือให้ active เลยตามนโยบาย
        # ตาม Requirement ของคุณ: "ระบุข้อมูลที่เพิ่งถูกดึงเข้ามา... ในวันแรก" -> ควรเป็น NEW
        return STATUS_NEW

    @staticmethod
    def should_promote_to_active(row_data: Dict[str, Any]) -> bool:
        """
        ตรวจสอบว่าข้อมูลสถานะ 'new' พร้อมที่จะเปลี่ยนเป็น 'active' หรือยัง
        Criteria: ต้องมี Ticker, Source และ Name ที่ถูกต้อง
        """
        ticker = row_data.get('ticker')
        name = row_data.get('name')
        
        # Validation Logic พื้นฐาน
        has_ticker = ticker and str(ticker).strip() != ""
        has_name = name and str(name).strip() not in ["", "None", "NaN", "N/A"]
        
        return has_ticker and has_name

    @staticmethod
    def should_mark_inactive(last_seen_str: str) -> bool:
        """
        ตรวจสอบว่าควรเปลี่ยนเป็น 'inactive' หรือไม่ (สำหรับเช็ครายตัว)
        """
        if not last_seen_str:
            return True # ถ้าไม่มีวันที่ last_seen เลย ให้ inactive ไปเลย
            
        try:
            last_seen = datetime.strptime(last_seen_str, "%Y-%m-%d")
            cutoff_date = datetime.now() - timedelta(days=INACTIVE_THRESHOLD_DAYS)
            
            # ถ้า last_seen เก่ากว่า cutoff (เช่น หายไป 8 วัน) -> True (Inactive)
            # ถ้า last_seen อยู่ในช่วง 1-7 วัน -> False (ยัง Active อยู่)
            return last_seen < cutoff_date
            
        except ValueError:
            return False

# ==========================================
# SQL GENERATOR HELPERS (สำหรับใช้ใน Script Sync)
# ==========================================

    @staticmethod
    def get_sql_update_inactive(table_name: str = "stg_security_master") -> str:
        """
        สร้าง SQL Query สำหรับรันใน Batch Job เพื่อเปลี่ยน status เป็น inactive
        โดยใช้ Logic 7 วันเดียวกัน
        """
        # หมายเหตุ: ใช้ Parameter binding (:cutoff_date) ในการรันจริงเพื่อความปลอดภัย
        return f"""
            UPDATE {table_name}
            SET 
                status = '{STATUS_INACTIVE}',
                updated_at = NOW()
            WHERE 
                status = '{STATUS_ACTIVE}' 
                AND last_seen < :cutoff_date
        """

    @staticmethod
    def get_sql_promote_new_to_active(table_name: str = "stg_security_master") -> str:
        """
        สร้าง SQL Query สำหรับเปลี่ยน new -> active เมื่อข้อมูลครบ
        """
        return f"""
            UPDATE {table_name}
            SET 
                status = '{STATUS_ACTIVE}',
                updated_at = NOW()
            WHERE 
                status = '{STATUS_NEW}'
                AND name IS NOT NULL 
                AND name != '' 
                AND name != 'N/A'
        """