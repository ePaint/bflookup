import os

import pandas

from src.OutputEntry import OutputEntry
from src.settings import SETTINGS


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
    dataframe = pandas.read_csv(f"{folder_name}/{files[0]}", keep_default_na=False)
    mark_as_processed(folder_name=folder_name, file_name=files[0])
    return dataframe


def read_data():
    data = read_first_file_in_folder(folder_name=SETTINGS.DATA_FOLDER)
    data = data.assign(UPC=data["UPC"].str.split(",")).explode("UPC")
    data["UPC"] = pandas.to_numeric(data["UPC"], errors="coerce")
    data["UPC"] = data["UPC"].round().astype(int)
    data = data.drop_duplicates(subset=["ID", "UPC"]).reset_index(drop=True)
    return data


def read_lookup() -> dict[str, int]:
    data = read_first_file_in_folder(folder_name=SETTINGS.LOOKUP_FOLDER)
    data = data.groupby("UPC").agg({"Quantity": "sum"}).reset_index()
    output = {}
    for index, row in data.iterrows():
        output[row["UPC"]] = int(row["Quantity"])
    return output


def save_output(data: list[OutputEntry]):
    output_df = pandas.DataFrame(data)
    filename = f"{SETTINGS.OUTPUT_FOLDER}/{SETTINGS.TIMESTAMP}_output.csv"

    def format_currency(value):
        if value > 0:
            return f"${value:.2f}"
        elif value < 0:
            return f"(${abs(value):.2f})"
        else:
            return "$-"

    for column in OutputEntry.get_currency_fields():
        output_df[column] = output_df[column].apply(format_currency)
    output_df["UPC"] = "'" + output_df["UPC"].astype(str)
    output_df["Stock Code"] = "'" + output_df["Stock Code"].astype(str)
    output_df.to_csv(filename, index=False)


def lookup_upc(upc: str, data: pandas.DataFrame, column: str = "UPC") -> pandas.DataFrame:
    return data[data[column] == upc]


def main():
    data = read_data()
    lookup = read_lookup()

    output = []
    for upc, quantity in lookup.items():
        results = lookup_upc(upc=upc, data=data)
        if len(results) > 1:
            raise ValueError(f"Multiple results found for UPC: {upc}")

        if results.empty:
            continue

        entry = OutputEntry()
        entry.upc = str(upc)
        entry.qty_input = quantity
        result = results.iloc[0]
        entry.qty_database = int(float(result["Total Qty On Hand"]))
        entry.unit_cost = float(result["Latest Cost"])
        entry.stock_code = result["Stock Code"]
        entry.name = result["Name"]
        entry.category = result["Category Name"]
        entry.category_group = result["Category Group Name"]

        entry.unit_variance = entry.qty_input - entry.qty_database
        entry.dollar_variance = round(entry.unit_variance * entry.unit_cost, 2)
        output.append(entry.model_dump(by_alias=True))

    save_output(data=output)


if __name__ == "__main__":
    main()
