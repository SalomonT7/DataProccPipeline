from csv_header_utils import clean_field_name, update_section
from csv_type_utils import guess_type
# What is this file for?
# This is the main report building file
def get_column_values(data_rows, col_index):
    column_values = []

    for row in data_rows:
        if col_index < len(row):
            value = row[col_index].strip()
        else:
            value = ""

        column_values.append(value)

    return column_values


def count_blanks(values):
    blank_count = 0

    for value in values:
        if value == "":
            blank_count += 1

    return blank_count


def get_sample_values(values, max_samples=3):
    sample_values = []

    for value in values:
        if value != "":
            sample_values.append(value)

        if len(sample_values) == max_samples:
            break

    return sample_values


def make_name_unique(name, used_names):
    if name not in used_names:
        used_names[name] = 1
        return name

    used_names[name] += 1
    return f"{name}_{used_names[name]}"


def build_column_report(headers, data_rows):
    report = []
    current_section = ""
    used_names = {}

    for col_index, header in enumerate(headers):
        current_section = update_section(header, current_section)

        column_values = get_column_values(data_rows, col_index)
        blank_count = count_blanks(column_values)
        sample_values = get_sample_values(column_values)

        clean_name = clean_field_name(header, current_section)
        clean_name = make_name_unique(clean_name, used_names)

        guessed_type = guess_type(column_values)

        report.append({
            "column_position": col_index,
            "original_header": header.strip(),
            "clean_field_name": clean_name,
            "guessed_type": guessed_type,
            "blank_count": blank_count,
            "sample_values": sample_values
        })

    return report


def build_summary(report, row_count, column_count):
    all_fields = []
    number_fields = []
    boolean_fields = []
    date_fields = []
    currency_fields = []
    suggested_required_fields = []

    for item in report:
        field_name = item["clean_field_name"]
        field_type = item["guessed_type"]
        blank_count = item["blank_count"]

        all_fields.append(field_name)

        if field_type in ["integer", "number"]:
            number_fields.append(field_name)

        if field_type == "boolean":
            boolean_fields.append(field_name)

        if field_type == "date":
            date_fields.append(field_name)

        if field_type == "currency":
            currency_fields.append(field_name)

        if blank_count == 0:
            suggested_required_fields.append(field_name)

    return {
        "row_count": row_count,
        "column_count": column_count,
        "columns": report,
        "suggested_all_fields": all_fields,
        "suggested_number_fields": number_fields,
        "suggested_boolean_fields": boolean_fields,
        "suggested_date_fields": date_fields,
        "suggested_currency_fields": currency_fields,
        "suggested_required_fields": suggested_required_fields
    }