import os
import yaml
import shutil

def read_yamlfile(config_path:str) ->dict:
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config

def createDirectory(directoryPath):
    if not os.path.isdir(directoryPath):
        os.mkdir(directoryPath)

def deleteDiretory(directoryPath):
    if os.path.isdir(directoryPath):
        shutil.rmtree(directoryPath)