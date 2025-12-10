## cross check pending

import pandas as pd
import logging
import numpy as np

logging.basicConfig(
    filename="logs/get_vendor_summary.log", 
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s", 
    filemode="a"  
)

def ingest_db(df, table_name, engine):
    """Load DataFrame into MySQL table"""
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)


def create_vendor_summary(engine):
    """Pull summary table from MySQL using SQL joins"""
    vendor_sales_summary = pd.read_sql_query("""
    WITH freight_summary AS (
        SELECT VendorNumber, SUM(freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),

    purchase_summary AS (
        SELECT 
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Volume,
            pp.Price AS ActualPrice,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp 
            ON pp.Brand = p.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY 1,2,3,4,5,6,7
    ),

    sales_summary AS (
        SELECT 
            VendorNo,
            Brand,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY 1,2
    )

    SELECT 
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM purchase_summary ps
    LEFT JOIN sales_summary ss 
        ON ss.VendorNo = ps.VendorNumber 
       AND ss.Brand = ps.Brand
    LEFT JOIN freight_summary fs 
        ON fs.VendorNumber = ps.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """, engine)

    return vendor_sales_summary


def clean_data(df):
    """Clean and enrich the vendor summary dataframe"""

    # Convert Volume to float safely
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")

    # Fill missing values with 0
    df.fillna(0, inplace=True)

    # Strip text values
    df["VendorName"] = df["VendorName"].str.strip()
    df["Description"] = df["Description"].str.strip()

    # Calculated metrics with safe division
    df["GrossProfit"] = df["TotalSalesDollars"] - df["TotalPurchaseDollars"]

    df["ProfitMargin"] = df["GrossProfit"].div(
        df["TotalSalesDollars"].replace(0, np.nan)
    ) * 100

    df["StockTurnover"] = df["TotalSalesQuantity"].div(
        df["TotalPurchaseQuantity"].replace(0, np.nan)
    )

    df["SalesToPurchaseRatio"] = df["TotalSalesDollars"].div(
        df["TotalPurchaseDollars"].replace(0, np.nan)
    )

    # Replace inf / -inf with 0
    df.replace([np.inf, -np.inf], 0, inplace=True)

    return df
