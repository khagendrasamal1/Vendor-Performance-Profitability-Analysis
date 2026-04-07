import sqlite3
import pandas as pd
import numpy as np
import logging

# 1. Setup Logging
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    """
    CRITICAL OPTIMIZATION: 
    We create indexes first. This prevents SQLite from loading 
    the entire 1.6GB table into RAM to find matches.
    """
    logging.info("Optimizing Database Indexes...")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_v_b ON sales(VendorNo, Brand)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_purch_v_b ON purchases(VendorNumber, Brand)")
    
    query = """
    WITH FreightSummary AS (
        SELECT VendorNumber, SUM(Freight) AS FreightCost
        FROM vendor_invoice GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT 
            p.VendorNumber, p.VendorName, p.Brand, p.Description,
            p.PurchasePrice, pp.Price AS ActualPrice, pp.Volume, 
            SUM(p.Quantity) AS TotalPurchaseQuantity, 
            SUM(p.Dollars) AS TotalPurchaseDollars 
        FROM purchases p 
        JOIN purchase_prices pp ON pp.Brand = p.Brand 
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),
    SalesSummary AS (
        SELECT 
            VendorNo, Brand, 
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT 
        ps.*, 
        ss.TotalSalesQuantity, ss.TotalSalesDollars, ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs ON ps.VendorNumber = fs.VendorNumber;
    """
    logging.info("Executing SQL Join...")
    return pd.read_sql_query(query, conn)

def clean_and_calculate(df):
    """
    OPTIMIZATION:
    Using np.where is significantly faster and uses less memory 
    than .replace() or .apply() for large datasets.
    """
    logging.info("Cleaning strings and filling nulls...")
    df.fillna(0, inplace=True)
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)

    logging.info("Calculating Financial KPIs...")
    # Basic Math
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']

    # Vectorized Math (Safe Division)
    # This prevents 'inf' values which crash some plotting/ingestion tools
    df['ProfitMargin'] = np.where(df['TotalSalesDollars'] != 0, 
                                  (df['GrossProfit'] / df['TotalSalesDollars']) * 100, 0)
    
    df['StockTurnover'] = np.where(df['TotalPurchaseQuantity'] != 0, 
                                   df['TotalSalesQuantity'] / df['TotalPurchaseQuantity'], 0)
    
    df['SalesToPurchaseRatio'] = np.where(df['TotalPurchaseDollars'] != 0, 
                                          df['TotalSalesDollars'] / df['TotalPurchaseDollars'], 0)
    return df

if __name__ == '__main__':
    try:
        conn = sqlite3.connect('inventory.db')
        
        # Step 1: Get Data
        summary_df = create_vendor_summary(conn)
        logging.info(f"Successfully fetched {len(summary_df)} records.")

        # Step 2: Clean & Add Columns
        final_df = clean_and_calculate(summary_df)

        # Step 3: Load back to DB
        logging.info("Ingesting final summary into database...")
        final_df.to_sql('vendor_sales_summary', conn, if_exists='replace', index=False)
        
        logging.info("Pipeline Complete!")
        print("Success! Vendor summary is ready in the database.")
        
    except Exception as e:
        logging.error(f"Kernel Error Avoided - Logic Failed: {e}")
        print(f"Error: {e}")
    finally:
        conn.close()