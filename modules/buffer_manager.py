import os
import glob
import pandas as pd
from datetime import datetime
from modules.clickhouse_client import insert_df

class BufferManager:
    def __init__(self, buffer_dir_name='buffer'):
        # Buffer dir is relative to the script execution or absolute? 
        # Let's make it relative to the project root/data/buffer to be centralized
        # Or keep it decentralized. Centralized is better for an orchestrator.
        self.project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.buffer_dir = os.path.join(self.project_root, 'data', 'buffer')
        
    def save(self, df: pd.DataFrame, prefix: str):
        if df.empty:
            return
        
        if not os.path.exists(self.buffer_dir):
            os.makedirs(self.buffer_dir)
        
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"{prefix}_{timestamp_str}.parquet"
        filepath = os.path.join(self.buffer_dir, filename)
        
        # Ensure consistent types if needed, but parquet is usually good
        df.to_parquet(filepath)
        print(f"[{prefix}] Buffered {len(df)} rows to {filename}")

    def flush(self, prefix: str, table_name: str):
        """
        Reads all parquet files matching the prefix, concatenates them, 
        and inserts into ClickHouse. Deletes files on success.
        """
        pattern = os.path.join(self.buffer_dir, f"{prefix}_*.parquet")
        files = sorted(glob.glob(pattern))
        
        if not files:
            return

        print(f"[{prefix}] Found {len(files)} buffered files. Attempting to flush...")
        
        dfs = []
        valid_files = []
        for f in files:
            try:
                dfs.append(pd.read_parquet(f))
                valid_files.append(f)
            except Exception as e:
                print(f"[{prefix}] Error reading {f}: {e}")
                continue
                
        if not dfs:
            return

        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Deduplicate if needed? 
        # Usually better to let ClickHouse handle it if using ReplacingMergeTree,
        # but we can do a simple drop_duplicates here to save bandwidth.
        # combined_df.drop_duplicates(inplace=True) 
        
        try:
            insert_df(table_name, combined_df)
            print(f"[{prefix}] Successfully inserted {len(combined_df)} rows into {table_name}")
            
            # Delete files only after successful insert
            for f in valid_files:
                try:
                    os.remove(f)
                except OSError as e:
                    print(f"[{prefix}] Error deleting {f}: {e}")
            print(f"[{prefix}] Buffer cleared.")
            
        except Exception as e:
            print(f"[{prefix}] ClickHouse unavailable or insert failed: {e}")
            print(f"[{prefix}] Data remains in buffer.")
