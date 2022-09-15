import os
import yaml
# from pprint import pprint

BASE_KEY = '_BASE_'

class ConfigLoader(dict):
    """
    Yaml parser to access config parameters with attribute or dict.
    """

    def __init__(self, config_file=None, data=None):
        if data is None:
            data = {}
        for k, v in data.items():
            setattr(self, k, v)
        if config_file:
            self.load_yaml(config_file)

    def __setattr__(self, name, value):
        if isinstance(value, dict):
            value = self.__class__(data=value)
        super(ConfigLoader, self).__setitem__(name, value)
        super(ConfigLoader, self).__setattr__(name, value)

    __setitem__ = __setattr__

    def update(self, e=None):
        data = e or dict()
        for k, v in data.items():
            setattr(self, k, v)

    def load_yaml(self, config_file):
        assert config_file.endswith(
            'yaml'), f'file is not yaml format : {config_file}'
        assert os.path.isfile(config_file), f'file not exist : "{config_file}"'
        with open(config_file, 'r') as fo:
            config_dict = yaml.safe_load(fo.read())
            self._load_base(config_file, config_dict)

    def _load_base(self, config_file, config_dict):
        if BASE_KEY in config_dict:
            print('gg')
            base_cfg_file = config_dict[BASE_KEY]
            if base_cfg_file.startswith("~"):
                base_cfg_file = os.path.expanduser(base_cfg_file)
            if not any(map(base_cfg_file.startswith, [".", "./", "/", "https://", "http://"])):
                # the path to base cfg is relative to the config file itself.
                base_cfg_file = os.path.join(
                    os.path.dirname(config_file), base_cfg_file)
            self.load_yaml(base_cfg_file)
            del config_dict[BASE_KEY]
        self.update(config_dict)

    def merge_from_file(self, config_file):
        self.load_yaml(config_file)

    def merge_from_dict(self, data):
        self.update(data)

def read_yaml_file(filepath):
    with open(filepath, "r") as stream:
        try:
            param_dict = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return param_dict