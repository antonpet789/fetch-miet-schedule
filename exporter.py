import pandas as pd
from pathlib import Path


def save_data(df: pd.DataFrame, file_path: str):
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fmt = path.suffix.lower()
    if fmt == '.csv':
        df.to_csv(path, index=False, encoding='utf-8-sig')
    elif fmt == '.json':
        df.to_json(path, orient='records', force_ascii=False, indent=4)
    elif fmt in ['.parquet', '.pq']:
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path.with_suffix('.csv'), index=False, encoding='utf-8-sig')
