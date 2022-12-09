import numpy as np
import pandas as pd

class Unpacker:
    """
    Class to flatten nested mongo documents to single level key-value mappings with nested keys represented using dot
    notation.

    """
    list_based_columns = list()
    failed_ids = list()

    def __init__(self, mongo_doc):
        self.mongo_doc = mongo_doc
        self.result = dict()
        self.unpack_nest_of_dict(self.mongo_doc)

    def unpack_nest_of_dict(self, data: dict, calling_key=""):
        for k, v in data.items():
            if isinstance(v, dict):
                key = f"{calling_key}{k}."
                self.unpack_nest_of_dict(v, calling_key=key)
            elif isinstance(k, str) and (isinstance(v, str) or isinstance(v, int)):
                flattened_element = f"{calling_key}{k}"
                self.result[flattened_element] = str(v)
            elif isinstance(v, list):
                flattened_element = f"{calling_key}{k}"
                self.list_based_columns.append(flattened_element)
                try:
                    flattened_data = pd.DataFrame(data=v)
                    flattened_data.replace(np.nan, "", inplace=True)
                    flattened_data = flattened_data.to_dict(orient="index")[0]
                    self.result.update(flattened_data)
                except:
                    self.failed_ids.append(self.result.get("_id", "NA"))

    @property
    def unpacked(self):
        return self.result

    @property
    def get_list_based_columns(self):
        return self.list_based_columns
