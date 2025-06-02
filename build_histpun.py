import pandas as pd
import glob
import os

# ─── 1) FOLDERS AND FILE‐PATS ────────────────────────────────────────────────────
# Change this path to wherever your decoded CSVs actually live.
decoded_folder = "csv_decoded"

# We assume each decoded file ends in "_decoded.csv"
decoded_files = glob.glob(os.path.join(decoded_folder, "*_decoded.csv"))

if len(decoded_files) == 0:
    raise FileNotFoundError(f"No files matching '*_decoded.csv' in {decoded_folder!r}.")

# ─── 2) READ + CONCAT ALL DECODED CSVs ───────────────────────────────────────────
df_list = []
for f in decoded_files:
    # Read everything in as strings first (so we don’t lose leading zeros or anything)
    df = pd.read_csv(f, dtype=str)
    df_list.append(df)

df_all = pd.concat(df_list, ignore_index=True)

# ─── 3) PARSE DATE‐FIELDS INTO DATETIME, EXTRACT YEAR ───────────────────────────
# If your decoded files truly have "DateReceived" in "YYYY-MM-DD" form,
# this will turn it into a pandas datetime; invalid or missing → NaT.
df_all["DateReceived"] = pd.to_datetime(df_all["DateReceived"], errors="coerce")

# Throw away any rows where DateReceived couldn’t be parsed—otherwise we can't
# assign them to a calendar year.
df_all = df_all[df_all["DateReceived"].notna()].copy()
df_all["Year"] = df_all["DateReceived"].dt.year

# ─── 4) COMPUTE AGE (YEARS) + AGE CATEGORY ─────────────────────────────────────
# If DateOfBirth is present in "YYYY-MM-DD", parse; else NaT.
df_all["DateOfBirth"] = pd.to_datetime(df_all["DateOfBirth"], errors="coerce")

# Compute age at reception (floor to an integer year)
df_all["Age"] = ((df_all["DateReceived"] - df_all["DateOfBirth"]).dt.days // 365).astype("Int64")

# Classify as "juvenile" if < 18, "adult" if ≥ 18, else "" if DateOfBirth was missing
def classify_age(val):
    if pd.isna(val):
        return ""
    return "juvenile" if val < 18 else "adult"

df_all["AgeCategory"] = df_all["Age"].apply(classify_age)

# ─── 5) PULL OUT A SIMPLE CRIME CATEGORY ────────────────────────────────────────
# The decoded "Crime" column often looks like "Larceny, degree 2nd", etc.
# We’ll take everything up to the first comma (lowercased) as the "CrimeCategory".
df_all["CrimeCategory"] = (
    df_all["Crime"]
      .fillna("")
      .apply(lambda s: s.split(",", 1)[0].strip().lower())
)

# ─── 6) SET UP A SOURCE‐KEY HELPER ───────────────────────────────────────────────
# Histpun requires a unique "Source" for each year. You can adjust this template:
def source_key(year):
    return f"NYInmateRecords{year}"

# ─── 7) AGGREGATE FOR EACH YEAR ─────────────────────────────────────────────────
histpun_rows = []

# We will iterate over each distinct year in ascending order
years = sorted(df_all["Year"].dropna().unique())

for year in years:
    year_df = df_all[df_all["Year"] == year].copy()
    src = source_key(int(year))
    
    # --- 7A) TOTAL PRISONER COUNT FOR THE YEAR (no qualifiers) ---
    total_count = len(year_df)
    histpun_rows.append({
        "Country":    "United States",
        "Year":       int(year),
        "Statistic":  "prisoners",
        "Value":      int(total_count),
        "Source":     src,
        "State":      "New York",
        "Race":       "",
        "Gender":     "",
        "Age":        "",
        "Crime":      "",
        "Institution": "",
        "Complete":   ""
    })
    
    # --- 7B) BY RACE × GENDER (Complete = "race,gender") ---
    # We assume RaceName, SexName exist and are not NaN—adjust names if yours differ.
    if "RaceName" in year_df.columns and "SexName" in year_df.columns:
        rg = (
            year_df.groupby(["RaceName", "SexName"])
                   .size()
                   .reset_index(name="Count")
        )
        # We'll mark every one of these as Complete="race,gender"
        # under the assumption that if you break down by race AND gender,
        # you truly have all categories for that year.
        for _, row in rg.iterrows():
            histpun_rows.append({
                "Country":    "United States",
                "Year":       int(year),
                "Statistic":  "prisoners",
                "Value":      int(row["Count"]),
                "Source":     src,
                "State":      "New York",
                "Race":       row["RaceName"].lower(),
                "Gender":     row["SexName"].lower(),
                "Age":        "",
                "Crime":      "",
                "Institution": "",
                "Complete":   "race,gender"
            })
    
    # --- 7C) BY AGE CATEGORY (Complete = "age" if both juvenile & adult appear) ---
    if "AgeCategory" in year_df.columns:
        age_grp = (
            year_df.groupby("AgeCategory")
                   .size()
                   .reset_index(name="Count")
        )
        cat_set = set(age_grp["AgeCategory"])
        age_complete = "age" if {"juvenile", "adult"}.issubset(cat_set) else ""
        for _, row in age_grp.iterrows():
            if row["AgeCategory"] != "":  # skip empty‐string rows
                histpun_rows.append({
                    "Country":    "United States",
                    "Year":       int(year),
                    "Statistic":  "prisoners",
                    "Value":      int(row["Count"]),
                    "Source":     src,
                    "State":      "New York",
                    "Race":       "",
                    "Gender":     "",
                    "Age":        row["AgeCategory"],
                    "Crime":      "",
                    "Institution": "",
                    "Complete":   age_complete
                })
    
    # --- 7D) BY CRIME CATEGORY (no Complete flagged) ---
    if "CrimeCategory" in year_df.columns:
        crime_grp = (
            year_df.groupby("CrimeCategory")
                   .size()
                   .reset_index(name="Count")
        )
        for _, row in crime_grp.iterrows():
            if row["CrimeCategory"] != "":  # skip any blank categories
                histpun_rows.append({
                    "Country":    "United States",
                    "Year":       int(year),
                    "Statistic":  "prisoners",
                    "Value":      int(row["Count"]),
                    "Source":     src,
                    "State":      "New York",
                    "Race":       "",
                    "Gender":     "",
                    "Age":        "",
                    "Crime":      row["CrimeCategory"],
                    "Institution": "",
                    "Complete":   ""
                })
    
    # --- 7E) BY INSTITUTION (no Complete flagged) ---
    if "Institution" in year_df.columns:
        inst_grp = (
            year_df.groupby("Institution")
                   .size()
                   .reset_index(name="Count")
        )
        for _, row in inst_grp.iterrows():
            inst_name = row["Institution"]
            if pd.notna(inst_name) and inst_name != "":
                histpun_rows.append({
                    "Country":    "United States",
                    "Year":       int(year),
                    "Statistic":  "prisoners",
                    "Value":      int(row["Count"]),
                    "Source":     src,
                    "State":      "New York",
                    "Race":       "",
                    "Gender":     "",
                    "Age":        "",
                    "Crime":      "",
                    "Institution": inst_name.lower(),
                    "Complete":   ""
                })

# ─── 8) BUILD FINAL DATAFRAME IN HISTPUN COLUMN ORDER ──────────────────────────
# ─── BUILD THE FINAL DataFrame (with safe column‐checking) ─────────────────────
histpun_df = pd.DataFrame(histpun_rows)

# The exact column order Histpun expects:
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

# If no rows were ever added, histpun_df might be empty with no columns at all.
# In that case, just create an empty DataFrame with the required columns:
if histpun_df.shape[0] == 0:
    histpun_df = pd.DataFrame(columns=histpun_columns)
else:
    # Otherwise, make sure every required column exists. If it’s missing, fill it with empty strings:
    for col in histpun_columns:
        if col not in histpun_df.columns:
            histpun_df[col] = ""
    # Now reorder to exactly match the spec:
    histpun_df = histpun_df[histpun_columns]

# ─── WRITE THE FINAL CSV ───────────────────────────────────────────────────────
output_path = "histpun_output.csv"
histpun_df.to_csv(output_path, index=False, encoding="utf-8")

print(f"✔️  Wrote {len(histpun_df)} rows to '{output_path}'")


# ─── 9) WRITE OUT TO CSV (UTF-8) ────────────────────────────────────────────────
output_path = "histpun_output.csv"
histpun_df.to_csv(output_path, index=False)

print(f"Written {len(histpun_df)} rows to '{output_path}'.")
print("Here are the first 10 lines:")
print(histpun_df.head(10))
