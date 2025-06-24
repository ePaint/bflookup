import os
import warnings

import pandas
from pandas.errors import SettingWithCopyWarning

from src.OutputEntry import OutputEntry
from src.settings import SETTINGS

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)


def mark_as_processed(
    folder_name: str,
    file_name: str,
) -> None:
    if not SETTINGS.MOVE_FILES_TO_PROCESSED_FOLDER:
        return
    if SETTINGS.ADD_TIMESTAMP_TO_PROCESSED_FILES:
        destination_file = f"{folder_name}/processed/{SETTINGS.TIMESTAMP}_{file_name}"
    else:
        destination_file = f"{folder_name}/processed/{file_name}"
    os.rename(src=f"{folder_name}/{file_name}", dst=destination_file)


def read_first_file_in_folder(
    folder_name: str,
) -> pandas.DataFrame:
    files = os.listdir(path=folder_name)
    files = [file for file in files if file != "processed"]
    if not files:
        raise FileNotFoundError("No files found in data folder")
    fullname = f'{folder_name}/{files[0]}'
    if not os.path.exists(fullname):
        raise FileNotFoundError(f"File not found: {fullname}")
    print(f"Reading file: {fullname}")
    dataframe = pandas.read_csv(fullname, keep_default_na=False)
    mark_as_processed(folder_name=folder_name, file_name=files[0])
    return dataframe


def read_data():
    data = read_first_file_in_folder(folder_name=SETTINGS.DATA_FOLDER)

    data = data.assign(UPC=data["UPC"].str.split(",")).explode("UPC")
    data["UPC"] = data["UPC"].str.replace(" ", "")
    data["UPC"] = pandas.to_numeric(data["UPC"], errors="coerce")
    data["UPC"] = data["UPC"].round()
    data = data.drop_duplicates(subset=["ID", "UPC"]).reset_index(drop=True)

    data = data.assign(StockCode=data["Stock Code"].str.split(",")).explode("StockCode")
    data["StockCode"] = data["StockCode"].str.replace(" ", "")
    data["StockCode"] = pandas.to_numeric(data["StockCode"], errors="coerce")
    data["StockCode"] = data["StockCode"].round()
    data = data.drop_duplicates(subset=["ID", "StockCode"]).reset_index(drop=True)

    return data


def read_lookup() -> dict[str, int]:
    data = read_first_file_in_folder(folder_name=SETTINGS.LOOKUP_FOLDER)
    data = data.groupby("UPC").agg({"Quantity": "sum"}).reset_index()
    output = {}
    for index, row in data.iterrows():
        output[row["UPC"]] = int(row["Quantity"])
    return output


def save_dataframe(dataframe: pandas.DataFrame, suffix: str = "output") -> None:
    filename = f"{SETTINGS.OUTPUT_FOLDER}/{SETTINGS.TIMESTAMP}_{suffix}.csv"

    def format_currency(value):
        if value > 0:
            return f"${value:.2f}"
        elif value < 0:
            return f"(${abs(value):.2f})"
        else:
            return "$-"

    for column in OutputEntry.get_currency_fields():
        dataframe[column] = dataframe[column].apply(format_currency)

    if SETTINGS.FORCE_UPC_EXCEL_STRING:
        if "UPC" in dataframe.columns:
            dataframe["UPC"] = "'" + dataframe["UPC"].astype(str)
        if "Stock Code" in dataframe.columns:
            dataframe["Stock Code"] = "'" + dataframe["Stock Code"].astype(str)
        if "Match UPC" in dataframe.columns:
            dataframe["Match UPC"] = "'" + dataframe["Match UPC"].astype(str)

    dataframe.to_csv(filename, index=False)


def lookup_upc(upc: str, data: pandas.DataFrame, columns: list[str]) -> (pandas.DataFrame, str):
    for column in columns:
        cmp_upc = upc
        result = data[data[column].astype(str).str.contains(cmp_upc)]
        if result.empty:
            cmp_upc = upc[1:]
            result = data[data[column].astype(str).str.contains(cmp_upc)]
        if result.empty:
            cmp_upc = upc[:-1]
            result = data[data[column].astype(str).str.contains(cmp_upc)]
        if result.empty:
            cmp_upc = upc[1:-1]
            result = data[data[column].astype(str).str.contains(cmp_upc)]
        if not result.empty:
            return result, cmp_upc
    return pandas.DataFrame(), upc


def main():
    data = read_data()
    lookup = read_lookup()

    output = []
    for upc, quantity in lookup.items():
        entry = OutputEntry()
        entry.upc = str(upc)
        entry.qty_input = quantity
        results, match_upc = lookup_upc(upc=str(upc), data=data, columns=["UPC", "StockCode"])
        # merge results by ID, get first of every field
        results = results.groupby("ID").first()
        if len(results) > 1:
            entry.error = (f"Multiple results found for UPC: '{upc}'. "
                           f"Match UPC substring case: '{match_upc}'. "
                           f"Conflicting item names: {results['Name'].tolist()}")

        if not results.empty:
            result = results.iloc[0]
            entry.found = True
            entry.match_upc = match_upc
            entry.qty_database = int(float(result["Total Qty On Hand"]))
            entry.unit_cost = float(result["Latest Cost"])
            entry.stock_code = result["Stock Code"]
            entry.name = result["Name"]
            entry.category = result["Category Name"]
            entry.category_group = result["Category Group Name"]

            entry.unit_variance = entry.qty_input - entry.qty_database
            entry.dollar_variance = round(entry.unit_variance * entry.unit_cost, 2)

        output.append(entry.model_dump(by_alias=True))

    output_dataframe = pandas.DataFrame(output)
    output_dataframe = output_dataframe.sort_values(by=["Found"], ascending=False)

    errors_dataframe = output_dataframe[output_dataframe["Found"] == False]

    save_dataframe(dataframe=output_dataframe)
    save_dataframe(dataframe=errors_dataframe, suffix="errors")


if __name__ == "__main__":
    main()
