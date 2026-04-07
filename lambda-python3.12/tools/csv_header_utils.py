import re


def clean_field_name(name, section=""):
    # make lowercase and remove extra spaces
    name = name.strip().lower()

    # turn special symbols into words or spaces
    name = name.replace("#", " number ")
    name = name.replace("&", " and ")
    name = name.replace("/", " ")
    name = name.replace("-", " ")
    name = name.replace("?", " ")
    name = name.replace(":", " ")
    name = name.replace("(", " ")
    name = name.replace(")", " ")

    # remove anything not a letter, number, or space
    name = re.sub(r"[^a-z0-9\s]", "", name)

    # turn spaces into underscores
    name = re.sub(r"\s+", "_", name).strip("_")

    # make repeated generic names unique using the section
    generic_names = {
        "weekday_number",
        "weekend_delivery",
        "ftl_10_or_more"
    }

    if section and name in generic_names:
        name = section + "_" + name

    return name


def update_section(header, current_section):
    header_lower = header.strip().lower()

    if "outbound" in header_lower or header_lower.startswith("ob "):
        return "outbound"

    if "inbound" in header_lower:
        return "inbound"

    return current_section