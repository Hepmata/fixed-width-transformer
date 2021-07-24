
def generate_fw_text_line(values: list, spacing: list):
    text = ""
    for itr in range(len(values)):
        append_text = (" " * (spacing[itr] - len(values[itr])))
        text += values[itr] + append_text
    return text
