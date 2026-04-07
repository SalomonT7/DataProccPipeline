import re
# What is this file for?
# The idea is that it will do type guessing to determing what columbs look like
def looks_like_boolean(value):
    value = value.strip().lower()

    boolean_words = {
        "true", "false",
        "yes", "no",
        "y", "n"
    }

    return value in boolean_words


def looks_like_date(value):
    value = value.strip()

    # examples: 1/4/2019 or 01/10/2019
    if re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", value):
        return True

    return False


def looks_like_currency(value):
    value = value.strip()

    # examples: $2,200.00 or $577.88
    if re.match(r"^\$[\d,]+(\.\d+)?$", value):
        return True

    return False


def clean_number_text(value):
    value = value.strip()
    value = value.replace(",", "")
    value = value.replace("$", "")
    return value


def looks_like_integer(value):
    value = clean_number_text(value)

    if re.match(r"^\d+$", value):
        return True

    return False


def looks_like_number(value):
    value = clean_number_text(value)

    if re.match(r"^\d+(\.\d+)?$", value):
        return True

    return False


def guess_type(values):
    non_blank_values = []

    for value in values:
        value = value.strip()
        if value != "":
            non_blank_values.append(value)

    if len(non_blank_values) == 0:
        return "string"

    if all(looks_like_boolean(v) for v in non_blank_values):
        return "boolean"

    if all(looks_like_date(v) for v in non_blank_values):
        return "date"

    if all(looks_like_currency(v) for v in non_blank_values):
        return "currency"

    if all(looks_like_integer(v) for v in non_blank_values):
        return "integer"

    if all(looks_like_number(v) for v in non_blank_values):
        return "number"

    return "string"