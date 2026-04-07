import csv
import json
import sys
from datetime import datetime
from pathlib import Path

from csv_report_utils import build_column_report, build_summary


def main():
    # figure out where this script lives
    script_folder = Path(__file__).resolve().parent
    project_folder = script_folder.parent
    data_folder = project_folder / "data"

    # if user gave a CSV path, use it
    # otherwise default to data/shipments.csv
    if len(sys.argv) == 2:
        csv_path = Path(sys.argv[1])
        if not csv_path.is_absolute():
            csv_path = project_folder / csv_path
    else:
        csv_path = Path("shipment-ingestion-pipeline/data/shipments.csv")

    print("Using CSV file:", csv_path)

    # make sure the file exists
    if not csv_path.exists():
        print("CSV file not found:", csv_path)
        return

    # read CSV
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as file:
        reader = csv.reader(file)
        all_rows = list(reader)

    if len(all_rows) < 2:
        print("CSV is empty or missing data.")
        return

    headers = all_rows[0]
    data_rows = all_rows[1:]

    # build report
    report = build_column_report(headers, data_rows)
    result = build_summary(report, len(data_rows), len(headers))

    # make sure data folder exists
    data_folder.mkdir(parents=True, exist_ok=True)

    # always create a brand-new file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = data_folder / f"csv_report_{timestamp}.json"

    with open(output_file, "x", encoding="utf-8") as file:
        json.dump(result, file, indent=2)

    print("Results saved to:", output_file)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()