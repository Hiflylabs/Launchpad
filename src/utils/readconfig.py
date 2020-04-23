import json

def read_config(path):
    """Read configuration in json file.

    Args:
        path (str): Path of the json file

    Returns:
        dict: dictionary of the configurations
    """

    with open(path, 'rb') as f:
        conf_dict = json.load(f)

    return conf_dict
