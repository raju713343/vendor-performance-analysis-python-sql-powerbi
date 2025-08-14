import sqlite3
import pandas as pd
import logging

# Getting the root logger
logger = logging.getLogger()
# Removing all existing handlers from the root logger to ensure only the specified configuration is used.
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

logging.basicConfig(
    filename = "logs/get_vendor_summary.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a"
)
def create_vendor_summary(conn):
    '''This table will merge the different tables to get the overall vendor summary, and add new columns in the resultant data'''
    vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS (
    SELECT 
        VendorNumber,
        sum(Freight) AS FreightCost 
    FROM vendor_invoice 
    GROUP BY VendorNumber
    ), 
    PurchaseSummary AS (
        SELECT 
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.PurchasePrice,
            pp.Description,
            pp.Volume,
            pp.Price AS ActualPrice,
            SUM(p.Quantity) AS TotalPurchaseQuantity, 
            SUM(p.Dollars) AS TotalPurchaseDollars 
        FROM Purchases AS p
        JOIN Purchase_prices AS pp 
        ON p.Brand = pp.Brand 
        WHERE p.PurchasePrice>0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand
        ORDER BY TotalPurchaseDollars
    ), 
    SaleSummary AS (
        SELECT 
            VendorNo, 
            Brand,
            SUM(SalesDollars) as TotalSalesDollars,
            SUM(SalesPrice) as TotalSalesPrice,
            SUM(SalesQuantity) as TotalSalesQuantity,
            SUM(ExciseTax) as TotalExciseTax
        FROM sales
        GROUP BY VendorNo,Brand
        ORDER BY TotalSalesDollars
    )
        SELECT
            ps.VendorNumber,
            ps.VendorName,
            ps.Brand,
            ps.Description,
            ps.Volume,
            ps.PurchasePrice,
            ps.ActualPrice,
            ps.TotalPurchaseQuantity,
            ps.TotalPurchaseDollars,
            ss.TotalSalesQuantity,
            ss.TotalSalesPrice,
            ss.TotalSalesDollars,
            ss.TotalExciseTax,
            fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SaleSummary ss
        ON ss.VendorNo = ps.VendorNumber AND ss.Brand = ps.Brand 
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC""",conn)
    return vendor_sales_summary
def clean_data(df):
    '''This Function will clean the data'''
    
    #Changing datatype to float
    df["Volume"] = df["Volume"].astype("float64")

    #Filling missing values with 0
    df.fillna(0,inplace=True)

    #Removing spaces from categorical columns
    df["VendorName"] = df["VendorName"].str.strip()
    df["Description"] = df["Description"].str.strip()

    #Creating new columns for better analysis
    df["GrossProfit"] = df["TotalSalesDollars"]-df["TotalPurchaseDollars"]
    df["ProfitMargin"] = (df["GrossProfit"]/df["TotalSalesDollars"])*100
    df["StockTurnover"] = df["TotalSalesQuantity"]/df["TotalPurchaseQuantity"]
    df["SalestoPurchaseRatio"] = df["TotalSalesDollars"]/df["TotalPurchaseDollars"]

    return df

def ingest_db(df,table_name,engine):
    '''This function will ingest dataframe into database'''
    df.to_sql(table_name,con=engine,if_exists="replace",index=False)

if __name__ == '__main__':
    #Creating Database connection
    conn = sqlite3.connect("inventory.db")

    logging.info("Creating vendor summary table.......")
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info("Cleaning Data.......")
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info("Ingesting data........")
    ingest_db(clean_df,"vendor_sales_summary",conn)
    logging.info("Completed")