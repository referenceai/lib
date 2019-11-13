import os
from os import path
from urllib import request
from abc import abstractmethod
import numpy as np
from enum import Enum

class DataSource():

    def __init__(self, id, base_path = None):
        self.id = id

        if base_path is not None:
            self.base_path = base_path
        else:
            self.base_path = os.path.join(".rai", "dataset", id)

        self.train_directory = path.join(self.base_path, "train")
        self.test_directory = path.join(self.base_path, "test")

        if not path.exists(self.base_path):
            os.makedirs(self.base_path)

    @abstractmethod
    def load(self, train_url: str, test_url = None) -> str:
        base_path = os.path.join(self.base_path, self.id)
        if not path.exists(base_path):
            # download and store files
            os.makedirs(path.join(base_path, "train"))
            request.urlretrieve(train_url, path.join(base_path,"train"))
            if test_url is not None:
                os.makedirs(path.join(base_path, "test"))
                request.urlretrieve(test_url, path.join(base_path, "test"))

    @abstractmethod
    def train_set(self) -> np.array:
        pass

    @abstractmethod
    def test_set(self) -> np.array:
        pass

class Image(np.ndarray):
    pass

class ImageScheme(Enum):
    BINARY = 1
    BW = 2
    RGB = 3
    RGBA = 4
    CYMK = 5
    HVS = 6

class ImagesDataSource(DataSource):
    
    def __init__(self, id, type: ImageScheme):
        super().__init__(id)
        self.scheme = type
        self.num_channels = None
        if type is ImageScheme.BINARY:
            self.num_channels = 1
        elif type is ImageScheme.BW:
            self.num_channels = 1
        elif type is ImageScheme.RGB:
            self.num_channels = 3
        elif type is ImageScheme.RGBA:
            self.num_channels = 4
        elif type is ImageScheme.CYMK:
            self.num_channels = 4
        elif type is ImageScheme.HVS:
            self.num_channels = 3

    def load_from_url(self, train_url: (str,str), test_url: (str,str)) -> str:
        super.load_from_url(train_url, test_url = test_url)
    
    def train_set(self) -> (np.ndarray, np.ndarray):
        pass

    def test_set(self) -> (np.ndarray, np.ndarray):
        pass

    def type(self):
        return self.scheme
    
    def channels(self):
        self.num_channels
    
    @abstractmethod
    def images(self):
        pass

    @abstractmethod
    def image(self, idx):
        pass

    @abstractmethod
    def num_images(self):
        pass
