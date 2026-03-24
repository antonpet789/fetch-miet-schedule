"""
Module: MIET Schedule Extractor
Description: Асинхронный инструмент для массового сбора расписания МИЭТ.
"""

import asyncio
import httpx
import pandas as pd
from tqdm.asyncio import tqdm

BASE_URL = "https://miet.ru/schedule"
DATA_URL = f"{BASE_URL}/data"
GROUPS_URL = f"{BASE_URL}/groups"
HEADERS = {"Referer": f"{BASE_URL}/"}

MAX_CONCURRENT_REQUESTS = 20  # чтобы не как DDoS


async def _fetch_group_data_async(
    client: httpx.AsyncClient, 
    group_name: str, 
    semaphore: asyncio.Semaphore,
    timeout: int, 
    retries: int
) -> tuple[str, list | None, str | None]:
    """Асинхронно скачивает данные одной группы с лимитом соединений."""
    last_error = None

    async with semaphore:
        for _ in range(retries):
            try:
                res = await client.get(
                    DATA_URL, 
                    params={'group': group_name}, 
                    timeout=timeout)
                res.raise_for_status()
                data = res.json().get('Data', [])
                return group_name, data, None
            except Exception as e:
                last_error = str(e)
                await asyncio.sleep(0.5) 

    return group_name, None, last_error


async def suck_out(thread_timeout_sec: int = 10, thread_retries_num: int = 3):
    """
    Основная функция для сбора данных.
    
    В Jupyter:
        df, failed = await suck_out()
        
    В обычном Python:
        df, failed = asyncio.run(suck_out())
    """
    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as client:
        try:
            print("[Info] Получение списка групп...")
            res = await client.get(GROUPS_URL, timeout=10)
            res.raise_for_status()
            group_names = res.json()
        except Exception as e:
            print(f"[Error] Не удалось получить список групп: {e}")
            return pd.DataFrame(), pd.DataFrame()

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        tasks = [
            _fetch_group_data_async(client, name, semaphore, thread_timeout_sec, thread_retries_num)
            for name in group_names
        ]

        print(f"[Info] Высасываем данные для {len(group_names)} групп...")
        results = await tqdm.gather(*tasks, desc="Async Scraping")

        all_records = []
        failed_groups = []

        for group_name, data, error in results:
            if data is not None:
                for entry in data:
                    entry['Group_Fixed_Name'] = group_name
                    all_records.append(entry)
            else:
                failed_groups.append({"group": group_name, "error": error})

        df = pd.json_normalize(all_records) if all_records else pd.DataFrame()
        failed_df = pd.DataFrame(failed_groups) if failed_groups else pd.DataFrame()

        print(f"[Info] Готово! Собрано записей: {len(df)}")
        if not failed_df.empty:
            print(f"[Warn] Ошибок: {len(failed_df)}")

        return df, failed_df
