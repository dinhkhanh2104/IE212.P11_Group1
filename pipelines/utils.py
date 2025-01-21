import yaml

# load kafka configs from yaml file
def load_kafka_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)