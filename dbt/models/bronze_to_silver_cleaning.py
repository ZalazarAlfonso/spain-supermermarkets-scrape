import pandas as pd
import re
import unicodedata

def norm(s):
    """
        Function for normalizing text columns.
        args:
            s: Text (category, subcategory, product)

        return:
            s: Normalized text.
    """
    if pd.isna(s): 
        return None
    s = str(s).lower().strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))  # remove accents
    s = re.sub(r"\s+", " ", s)
    return s

def model(dbt, session):
    # configuration for silver dataset
    dbt.config(materialized="table", schema="dwh_silver_dev")

    # tables reading.
    df1 = dbt.source("dwh_bronze_dev", "bronze_alcampo_products_p").to_pandas()
    df2 = dbt.source("dwh_bronze_dev", "bronze_carrefour_products_p").to_pandas()
    df3 = dbt.source("dwh_bronze_dev", "bronze_dia_products_p").to_pandas()
    df4 = dbt.source("dwh_bronze_dev", "bronze_mercadona_products_p").to_pandas()

    # Unifiying tables
    df_final = pd.concat([df1, df2, df3, df4])

    df_final.rename(
        columns={
            "category":"category_raw",
            "subcategory":"subcategory_raw",
            "product":"product_raw"
        }, inplace=True
    )

    df_final["category_norm"] = df_final["category_raw"].apply(norm)
    df_final["subcategory_norm"] = df_final["subcategory_raw"].apply(norm)
    df_final["product_norm"] = df_final["product_raw"].apply(norm)

    # Filtering out nulls
    print(f"Removing {len(df_final[~df_final['product_raw'].notna()])} rows with NA product names")
    df_final = df_final[df_final['product_raw'].notna()]
    print(f"Removing {len(df_final[~df_final['price'].notna()])} rows with NA prices")
    df_final = df_final[df_final['price'].notna()]

    # Removing 'donations'
    print(f"Removing {len(df_final[df_final['product_norm'].str.contains('donacion')])} rows donaciones")
    df_final = df_final[~df_final['product_norm'].str.contains('donacion')]

    # 4. Devolver el DataFrame final que dbt escribirá en BigQuery
    return df_final