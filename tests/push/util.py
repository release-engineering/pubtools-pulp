import re


def hide_unit_ids(text):
    return re.sub(r" unit_id: [^\n]+", " unit_id: (hidden)", text)
