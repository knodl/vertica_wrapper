import yaml

def read_yaml_config(config_path):
    """
    reads config from yaml file
    """
    with open(config_path, "r") as config_file:
        try:
            config = yaml.load(config_file)
            return config
        except yaml.YAMLError:
            raise Exception

        
if __name__ == "__main__":
    yml = read_yaml_config("vertica_config.yaml")
    print(yml["vertica"])