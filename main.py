import asyncio
from extractor import suck_out
from exporter import save_data

async def main():
    SAVE_PATH = "output/full_schedule.csv"
    df, failed = await suck_out()
    
    if not df.empty:
        save_data(df, SAVE_PATH)
        print(f"Success! Records: {len(df)}")
    
    if not failed.empty:
        print(f"Failed groups: {len(failed)}. Check them manually.")

if __name__ == "__main__":
    asyncio.run(main())
