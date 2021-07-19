
def generate_fw_text_line(values: list[str], spacing: list[int]):
    if len(values) != len(spacing):
        raise Exception("values and spacing must be of the same length")
    text = ""
    for itr in range(len(values)):
        text += values[itr] + (" "* (spacing[itr] - len(values[itr])))

    return text
