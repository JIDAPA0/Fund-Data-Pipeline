import shutil
from pathlib import Path
from datetime import datetime

# Simple archiver: copy hashed static detail files into an archive folder for traceability.

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
TODAY = datetime.now().strftime("%Y-%m-%d")
HASHED_DIR = BASE_DIR / "data" / "04_hashed" / "static_details" / TODAY
ARCHIVE_DIR = BASE_DIR / "data" / "archive" / "static_details" / TODAY


def main():
    if not HASHED_DIR.exists():
        print(f"⚠️ Hashed dir not found: {HASHED_DIR}")
        return
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    for f in HASHED_DIR.glob("*.csv"):
        shutil.copy2(f, ARCHIVE_DIR / f.name)
        print(f"📦 Archived {f.name}")


if __name__ == "__main__":
    main()
