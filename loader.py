import yfinance as yf
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

DATA_DIR = "data/parquet"

def flatten_yf_multiindex(df):


    df = df.rename_axis(columns=['field','ticker'])

    df_flat = df.stack(level='ticker', future_stack=True).reset_index()

    df_flat = df_flat.rename(columns={'level_1':'ticker', 'Date':'date'})
    return df_flat

def save_to_parquet(df):
    os.makedirs(DATA_DIR, exist_ok=True)
    print("Columns before save:", df.columns.tolist())
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=DATA_DIR,
        partition_cols=["ticker"],
        compression="zstd",
    )

if __name__ == "__main__":
    tickers = ["AAPL","MSFT","GOOG"]
    # 一次性下载，得到 MultiIndex
    raw = yf.download(tickers, start="2020-01-01", auto_adjust=True)
    # 扁平化
    df = flatten_yf_multiindex(raw)
    print(df.head())
    save_to_parquet(df)
    print("Saved to", DATA_DIR)