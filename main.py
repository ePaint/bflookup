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
    return data


def read_lookup() -> dict[str, int]:
    data = read_first_file_in_folder(folder_name=SETTINGS.LOOKUP_FOLDER)
    output = {}
    for index, row in data.iterrows():
        output[row["UPC"]] = int(row["Quantity"])
    return output


def save_output(data: list[OutputEntry]):
    output_df = pandas.DataFrame(data)
    output_df.to_csv(
        f"{SETTINGS.OUTPUT_FOLDER}/{SETTINGS.TIMESTAMP}_output.csv", index=False
    )


def main():
    data = read_data()
    lookup = read_lookup()

    output = []
    for upc, quantity in lookup.items():
        entry = OutputEntry(upc=upc, qty_input=quantity)

        results = data[data["UPC"] == upc]
        if len(results) > 1:
            raise ValueError(f"Multiple results found for UPC: {upc}")

        if not results.empty:
            result = results.iloc[0]
            entry.qty_database = int(result["Qty On Hand"])
            entry.unit_cost = float(result["Unit Cost"])
            entry.name = result["Name"]

        entry.variance = entry.qty_input - entry.qty_database
        entry.total_dollar_variance = round(entry.variance * entry.unit_cost, 2)
        output.append(entry)

    save_output(data=output)


if __name__ == "__main__":
    main()
