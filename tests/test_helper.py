
def generate_fw_text_line(values: list, spacing: list):
    text = ""
    for itr in range(len(values)):
        append_text = (" " * (spacing[itr] - len(values[itr])))
        text += values[itr] + append_text
    return text


def generate_file_data(file_name, mapping: dict):
    """
    header: {
        "values": [[1, 2, 3, 4, 5]]
        "spacing": [5, 5, 5, 5, 5]
    },
    body: {
        "values": [[1, 2, 3, 4, 5],[1, 2, 3, 4, 5]]
        "spacing: [5, 5, 5, 5, 5]
    }

    """
    with open(file_name, 'w') as file:
        for m in mapping:
            target = mapping[m]
            for v in target['values']:
                file.write(generate_fw_text_line(v, target['spacing']))
                file.write("\n")

