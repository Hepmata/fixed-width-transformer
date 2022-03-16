import hashlib
import os
import traceback


def generate_csv_file(content: dict, file_name: str):
    if os.path.isfile(file_name):
        os.remove(file_name)
    with open(file_name, 'w') as file:
        if 'header' in content:
            file.write(f"{content['header']}\n")
        if 'body' in content:
            if isinstance(content['body'], str):
                file.write(f"{content['body']}\n")
            else:
                for body in content['body']:
                    file.write(f'{body}\n')
        if 'footer' in content:
            file.write(f"{content['footer']}\n")


def create_config_file(content, file_name: str):
    if os.path.isfile(file_name):
        os.remove(file_name)
    with open(file_name, 'w') as file:
        file.write(content)


def create_fixed_width_file(content: dict, file_name: str):
    header_string = ''
    body_strings = ''
    footer_string = ''
    if 'header' in content:
        for header in content['header']:
            spacing = header['length'] - len(header['value'])
            if spacing < 0:
                raise Exception(
                    f"Content {header['name']} cannot be longer than the specified length of {header['length']}"
                )
            header_string += f"{header['value']}{' ' * spacing}"
    if 'body' in content:
        for body in content['body']:
            spacing = body['length'] - len(body['value'])
            if spacing < 0:
                raise Exception(
                    f"Content {body['name']} cannot be longer than the specified length of {body['length']}"
                )
            body_strings += f"{body['value']}{' ' * spacing}"

    if 'footer' in content:
        for footer in content['footer']:
            spacing = footer['length'] - len(footer['value'])
            if spacing < 0:
                raise Exception(
                    f"Content {footer['name']} cannot be longer than the specified length of {footer['length']}"
                )
            footer_string += f"{footer['value']}{' ' * spacing}"

    if os.path.isfile(file_name):
        os.remove(file_name)
    with open(file_name, 'w') as file:
        if header_string != '':
            file.write(f'{header_string}\n')
        if body_strings != '':
            file.write(f'{body_strings}\n')
        if footer_string != '':
            file.write(f'{footer_string}\n')


def create_multiline_fixed_width_file(content: dict, file_name: str):
    def write_to_file(f, s, c):
        s = s - len(c)
        if s < 0:
            raise Exception(f'Content length exceeds spacing. Content is {c}')
        f.write(f"{c}{' ' * s}")

    try:
        with open(file_name, 'w') as file:
            for idx, segment_key in enumerate(content):
                for idx2, content_key in enumerate(content[segment_key]):
                    if type(content_key) == list:
                        for array_content in content_key:
                            write_to_file(file, array_content['length'], array_content['value'])
                        if idx2 + 1 != len(content[segment_key]):
                            file.write('\n')
                    else:
                        write_to_file(file, content_key['length'], content_key['value'])
                if idx + 1 != len(content):
                    file.write('\n')
    except Exception as e:
        traceback.print_exc(e)
        delete_file(file_name)


def create_file(content: str, file_name: str):
    with open(file_name, 'w') as file:
        file.write(content)


def generate_hash_from_file(file_name: str, hash_file_name: str = ''):
    BLOCK_SIZE = 65536
    hash = hashlib.sha256()
    with open(file_name, 'rb') as file:
        fb = file.read(BLOCK_SIZE)
        while len(fb) > 0:
            hash.update(fb)
            fb = file.read(BLOCK_SIZE)
    if len(hash_file_name) > 0:
        create_file(hash.hexdigest(), hash_file_name)
    return hash.hexdigest()


def delete_file(file_name):
    if type(file_name) == str:
        os.remove(file_name)
    elif type(file_name) == list:
        for f in file_name:
            os.remove(f)


def build_file_content(length_format: dict, data: dict):
    content = {}
    for key in data:
        if key not in length_format.keys():
            raise ValueError(f'Corresponding length format not found for key {key}')
        c1 = []
        fmt = length_format[key]
        for idx1, v1 in enumerate(data[key]):
            if type(v1) == list:
                if len(v1) != len(fmt):
                    raise Exception(f'Data {v1} length does not match format length of {len(fmt)}')
                c2 = []
                for idx2, v2 in enumerate(v1):
                    # multiline config
                    c2.append({'length': fmt[idx2], 'value': v2})
                c1.append(c2)
            else:
                c1.append({'length': fmt[idx1], 'value': v1})
        content[key] = c1
    return content
