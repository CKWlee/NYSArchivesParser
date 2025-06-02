# build_histpun_debug.py

import pandas as pd
import glob
import os

# ─── (A) WHERE ARE THE DECODED CSVs? ────────────────────────────────────────────
decoded_folder = "csv_decoded"  
decoded_pattern = os.path.join(decoded_folder, "*_decoded.csv")

decoded_files = glob.glob(decoded_pattern)

# [DEBUG] Print out exactly which files we found (if any)
print(f"[DEBUG] Looking in folder: '{decoded_folder}'")
print(f"[DEBUG] Files matching '*_decoded.csv': {decoded_files}\n")

if not decoded_files:
    raise FileNotFoundError(f"No files found matching '{decoded_pattern}'")

# ─── (B) READ + CONCAT ALL DECODED CSVs ─────────────────────────────────────────
df_list = []
for fpath in decoded_files:
    print(f"[DEBUG] Reading decoded file: {fpath}")
    df = pd.read_csv(fpath, dtype=str)
    df_list.append(df)

df_all = pd.concat(df_list, ignore_index=True)

# [DEBUG] Show how many total rows we got from concatenation
print(f"\n[DEBUG] Total concatenated rows (before date filtering): {len(df_all)}")
print("[DEBUG] Columns present in df_all:", list(df_all.columns), "\n")

# ─── (C) PARSE DATES, EXTRACT YEAR ─────────────────────────────────────────────
df_all["DateReceived"] = pd.to_datetime(df_all["DateReceived"], errors="coerce")

# [DEBUG] Count how many rows have a valid DateReceived vs. how many are dropped
valid_date_count = df_all["DateReceived"].notna().sum()
invalid_date_count = df_all["DateReceived"].isna().sum()
print(f"[DEBUG] Rows with valid DateReceived: {valid_date_count}")
print(f"[DEBUG] Rows with INVALID DateReceived (will be dropped): {invalid_date_count}\n")

# Drop rows without a valid DateReceived
df_all = df_all[df_all["DateReceived"].notna()].copy()
df_all["Year"] = df_all["DateReceived"].dt.year

# [DEBUG] Show the first few rows (with Year) so you can eyeball them
print("[DEBUG] Sample of df_all after parsing DateReceived:")
print(df_all[["DateReceived", "Year"]].head(10), "\n")

# ─── (D) COMPUTE AGE + AGE CATEGORY ───────────────────────────────────────────
df_all["DateOfBirth"] = pd.to_datetime(df_all["DateOfBirth"], errors="coerce")
df_all["Age"] = ((df_all["DateReceived"] - df_all["DateOfBirth"]).dt.days // 365).astype("Int64")

def classify_age(age):
    if pd.isna(age):
        return ""
    return "juvenile" if age < 18 else "adult"

df_all["AgeCategory"] = df_all["Age"].apply(classify_age)

# ─── (E) SIMPLIFY CRIME INTO CrimeCategory ─────────────────────────────────────
df_all["CrimeCategory"] = (
    df_all["Crime"]
      .fillna("")
      .apply(lambda s: s.split(",", 1)[0].strip().lower())
)

# ─── (F) HELPER: MAKE A UNIQUE SOURCE KEY PER YEAR ──────────────────────────────
def source_key(year):
    return f"NYInmateRecords{year}"

# ─── (G) AGGREGATE YEAR BY YEAR ────────────────────────────────────────────────
histpun_rows = []
years = sorted(df_all["Year"].dropna().unique())

print(f"[DEBUG] Years found (after filtering by valid DateReceived): {years}\n")

for yr in years:
    year_df = df_all[df_all["Year"] == yr].copy()
    src    = source_key(int(yr))
    
    # (1) TOTAL prisoners for that year (no qualifiers)
    total_count = len(year_df)
    histpun_rows.append({
        "Country":     "United States",
        "Year":        int(yr),
        "Statistic":   "prisoners",
        "Value":       int(total_count),
        "Source":      src,
        "State":       "New York",
        "Race":        "",
        "Gender":      "",
        "Age":         "",
        "Crime":       "",
        "Institution": "",
        "Complete":    ""
    })
    
    # (2) BY RACE × GENDER (Complete = "race,gender")
    if "RaceName" in year_df.columns and "SexName" in year_df.columns:
        rg = (
            year_df
            .groupby(["RaceName", "SexName"])
            .size()
            .reset_index(name="Count")
        )
        print(f"[DEBUG] Year {yr} → found {len(rg)} Race×Gender groups")
        for _, row in rg.iterrows():
            histpun_rows.append({
                "Country":     "United States",
                "Year":        int(yr),
                "Statistic":   "prisoners",
                "Value":       int(row["Count"]),
                "Source":      src,
                "State":       "New York",
                "Race":        row["RaceName"].strip().lower(),
                "Gender":      row["SexName"].strip().lower(),
                "Age":         "",
                "Crime":       "",
                "Institution": "",
                "Complete":    "race,gender"
            })
    
    # (3) BY AGE CATEGORY (Complete="age" if juvenile & adult exist)
    if "AgeCategory" in year_df.columns:
        age_grp = (
            year_df
            .groupby("AgeCategory")
            .size()
            .reset_index(name="Count")
        )
        print(f"[DEBUG] Year {yr} → found {len(age_grp)} AgeCategory groups: {list(age_grp['AgeCategory'])}")
        cats = set(age_grp["AgeCategory"])
        age_complete = "age" if {"juvenile", "adult"}.issubset(cats) else ""
        for _, row in age_grp.iterrows():
            if row["AgeCategory"]:
                histpun_rows.append({
                    "Country":     "United States",
                    "Year":        int(yr),
                    "Statistic":   "prisoners",
                    "Value":       int(row["Count"]),
                    "Source":      src,
                    "State":       "New York",
                    "Race":        "",
                    "Gender":      "",
                    "Age":         row["AgeCategory"],
                    "Crime":       "",
                    "Institution": "",
                    "Complete":    age_complete
                })
    
    # (4) BY CrimeCategory (no Complete flag)
    if "CrimeCategory" in year_df.columns:
        crime_grp = (
            year_df
            .groupby("CrimeCategory")
            .size()
            .reset_index(name="Count")
        )
        print(f"[DEBUG] Year {yr} → found {len(crime_grp)} CrimeCategory groups")
        for _, row in crime_grp.iterrows():
            cat = row["CrimeCategory"]
            if cat:
                histpun_rows.append({
                    "Country":     "United States",
                    "Year":        int(yr),
                    "Statistic":   "prisoners",
                    "Value":       int(row["Count"]),
                    "Source":      src,
                    "State":       "New York",
                    "Race":        "",
                    "Gender":      "",
                    "Age":         "",
                    "Crime":       cat,
                    "Institution": "",
                    "Complete":    ""
                })
    
    # (5) BY Institution (no Complete flag)
    if "Institution" in year_df.columns:
        inst_grp = (
            year_df
            .groupby("Institution")
            .size()
            .reset_index(name="Count")
        )
        print(f"[DEBUG] Year {yr} → found {len(inst_grp)} Institution groups")
        for _, row in inst_grp.iterrows():
            inst = row["Institution"]
            if pd.notna(inst) and inst.strip():
                histpun_rows.append({
                    "Country":     "United States",
                    "Year":        int(yr),
                    "Statistic":   "prisoners",
                    "Value":       int(row["Count"]),
                    "Source":      src,
                    "State":       "New York",
                    "Race":        "",
                    "Gender":      "",
                    "Age":         "",
                    "Crime":       "",
                    "Institution": inst.strip().lower(),
                    "Complete":    ""
                })

# ─── (H) BUILD THE FINAL DataFrame (with safe‐column checking) ───────────────────
histpun_columns = [
    "Country",
    "Year",
    "Statistic",
    "Value",
    "Source",
    "State",
    "Race",
    "Gender",
    "Age",
    "Crime",
    "Institution",
    "Complete"
]

# Create the DataFrame from our rows list (which might be empty)
histpun_df = pd.DataFrame(histpun_rows)

# If we never appended any rows, we still want a DataFrame with the correct headers
if histpun_df.shape[0] == 0:
    print("[DEBUG] ==> No rows were generated (histpun_rows is empty). Making an empty DataFrame with the correct column headers.")
    histpun_df = pd.DataFrame(columns=histpun_columns)
else:
    # Otherwise, make sure each required column is present
    for col in histpun_columns:
        if col not in histpun_df.columns:
            print(f"[DEBUG] Column '{col}' was not in histpun_df; creating an empty column for it.")
            histpun_df[col] = ""
    # Reorder exactly in the Histpun spec
    histpun_df = histpun_df[histpun_columns]

# ─── (I) WRITE THE FINAL CSV ─────────────────────────────────────────────────────
output_path = "histpun_output.csv"
histpun_df.to_csv(output_path, index=False, encoding="utf-8")

print(f"\n✔️  Wrote {len(histpun_df)} rows to '{output_path}'")

