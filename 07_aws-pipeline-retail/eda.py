import marimo

__generated_with = "0.23.3"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path

    import polars as pl

    RETAIL_DATA_PATH = Path("./data/raw/online_retail.xlsx")
    TARGET_CSV_PATH = Path("./data/raw/online_retail.csv")
    TARGET_PARQUET_PATH = Path("./data/raw/online_retail.parquet")
    DATA_SCHEMA = pl.Schema({
        "InvoiceNo": pl.String,     # String to accommodate 'c' prefixes
        "StockCode": pl.String,     # String to accommodate alphanumeric codes
        "Description": pl.String,
        "Quantity": pl.Int64,
        "InvoiceDate": pl.Datetime(), # Assumed Datetime due to "day and time" description
        "UnitPrice": pl.Float64,
        "CustomerID": pl.Int64,
        "Country": pl.String,
    })
    return (
        DATA_SCHEMA,
        RETAIL_DATA_PATH,
        TARGET_CSV_PATH,
        TARGET_PARQUET_PATH,
        pl,
    )


@app.cell
def _(DATA_SCHEMA, RETAIL_DATA_PATH, TARGET_CSV_PATH, TARGET_PARQUET_PATH, pl):
    if TARGET_PARQUET_PATH.exists():
        lf = pl.scan_parquet(TARGET_PARQUET_PATH)
    elif TARGET_CSV_PATH.exists():
        lf = pl.scan_csv(TARGET_CSV_PATH)
        lf.sink_parquet(TARGET_PARQUET_PATH)
    elif RETAIL_DATA_PATH.exists():
        df = pl.read_excel(RETAIL_DATA_PATH, schema_overrides=DATA_SCHEMA)
        df.write_parquet(TARGET_PARQUET_PATH)
        df.write_csv(TARGET_CSV_PATH)
        ls = df.lazy()
    else:
        raise FileNotFoundError("No input file found")

    lf.head().collect()
    return (lf,)


@app.cell
def _(lf, pl):
    lf.select(pl.col("Country").unique().sort()).sink_csv("./data/raw/countries.csv")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
