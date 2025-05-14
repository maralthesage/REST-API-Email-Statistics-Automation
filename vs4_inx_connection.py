import pandas as pd
from datetime import datetime as dt
from creds import api

date = dt.datetime.today().strftime('%Y%m%d')

# --- Load Inxmail data ---
hg_path = api['hg-export']
jg_path = api['jg-export']

df_hg = pd.read_csv(hg_path)
df_jg = pd.read_csv(jg_path)

# Add newsletter type
df_hg["NL_TYPE"] = "HG"
df_jg["NL_TYPE"] = "JG"

# Combine and unify types for duplicates
df = pd.concat([df_hg, df_jg], ignore_index=True)
df["NL_TYPE"] = df.groupby("email")["NL_TYPE"].transform(lambda x: "-".join(sorted(set(x))))
df = df.drop_duplicates(subset="email").dropna(subset=["email"])

# Normalize email
df["email"] = df["email"].str.lower().str.strip()

# Select relevant columns
columns_to_keep = [
    "email", "Anrede", "Vorname", "Nachname", "Titel", "Firma",
    "Geburtsdatum", "NL_TYPE", "email_neu"
]
df = df[columns_to_keep]

# --- Load customer numbers from V2AD1009 ---
v2ad_path = "/Volumes/MARAL/CSV/F01/V2AD1009.csv"
v2ad = pd.read_csv(
    v2ad_path,
    sep=";",
    encoding="cp850",
    on_bad_lines="skip",
    usecols=["NUMMER", "EMAIL", "E_MAIL"]
)

# Normalize fields
v2ad["NUMMER"] = v2ad["NUMMER"].astype(str).str.replace(".0", "", regex=False).str.zfill(10)
v2ad["EMAIL"] = v2ad["EMAIL"].astype(str).str.strip()
v2ad["E_MAIL"] = v2ad["E_MAIL"].astype(str).str.strip()

# Use EMAIL where E_MAIL is missing
v2ad["E_MAIL"] = v2ad["E_MAIL"].fillna(v2ad["EMAIL"])
v2ad = v2ad.dropna(subset=["E_MAIL"])

# --- Merge email data with customer numbers ---
merged = df.merge(v2ad, left_on="email", right_on="E_MAIL", how="inner")
merged = merged.dropna(subset=["NUMMER"])
merged = merged.drop_duplicates()

# Final columns for export
output = merged[[
    "NUMMER", "E_MAIL", "Anrede", "Vorname", "Nachname", "Geburtsdatum", "NL_TYPE"
]]

# Export
output.to_excel(f"Inxmail-Kundennummer_{date}.xlsx", index=False)

# Optional: print count of unique customer numbers
print(f"Unique customer numbers: {output['NUMMER'].nunique()}")
