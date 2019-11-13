import pickle
import os
from os import path, makedirs
import shutil
from loguru import logger
from abc import abstractmethod
from typing import Callable
import numpy as np
import hashlib as hl
import keras

import inspect
from shutil import rmtree
from referenceai.utils.provider import DictionaryRing

import networkx as nx
import uuid
import datetime
import copy

from .datasource import DataSource
from .graph import GraphRing
from .transform import Transform


class PipelineAIProvider():
    @abstractmethod
    def hyperparameters(self) -> dict:
        pass

    @abstractmethod
    def optimizer(self) -> keras.optimizers.Optimizer:
        pass

    @abstractmethod
    def datasource(self) -> DataSource: 
        pass

    @abstractmethod
    def transform(self, ds: DataSource) -> (DataSource, Transform):
        pass

    @abstractmethod
    def inverse_transform(self, ds: DataSource) -> DataSource:
        pass

    @abstractmethod
    def model(self) -> keras.Model:
        pass

    @abstractmethod
    def train(self, ds: DataSource, model: keras.Model) -> keras.Model:
        pass

    @abstractmethod
    def save(self, model: keras.Model):
        pass

    @abstractmethod
    def load(self) -> keras.Model:
        pass

    @abstractmethod
    def resources(self):
        pass

    @abstractmethod
    def transform_one(self, item: np.array) -> (np.array, Transform):
        pass

    @abstractmethod
    def inverse_transform_one(self, labels : np.ndarray) -> str:
        pass


class Pipeline():

    base_cache_path = os.path.join(".rai","cache")

    def __init__(self, id = None):
        if id is None:
            self.id = str(uuid.uuid1())
        else:
            self.id = id
        
        self.rings = [GraphRing()]
        self.runs = [] # Array of dictionaries of the form {'ring' : <class GraphRing>,}
        
        if not path.exists(self.base_cache_path):
            makedirs(self.base_cache_path)
        
        self.base_path = os.path.join(self.base_cache_path, self.id)

    def revise(self):
        self.rings.insert(0, self.rings[0])
    
    def expunge(self):
        self.rings[0].expunge()

    def push(self, name, fns, revise = True):
        self.rings[0].push(name, fns)
        if revise: self.revise()

    def run(self, name, *args):
        rtn, fns_rtns = self.rings[0].run(name, *args)
        self.runs.insert(0, {'ring' : self.rings[0], 'stack' : fns_rtns})
        return rtn

    def restore_graph(self, ring_idx):
        self.rings.insert(0, self.rings[ring_idx])

    def serialize(self):
        with open(self.base_path, "wb") as f:
            pickle.dump({'rings' : self.rings, 'runs' : self.runs}, f)
    
    def deserialize(self):
        try:
            if path.exists(self.base_path):
                with open(self.base_path, "rb") as f:
                    info = pickle.load(f)
                    self.rings = info['rings']
                    self.runs = info['runs']
            else:
                logger.warning("Unable to deserialize graph ring at {path}. File does not exist.".format(path = self.base_path))
        except:
            # we are now going to remove the cache
            if path.exists(self.base_path):
                rmtree(self.base_path)
class PipelineAI(Pipeline):

    def __init__(self, id, provider: PipelineAIProvider):
        super().__init__(id = id)
        self.provider = provider
        self.push("train",
                      [self.provider.hyperparameters,
                       self.provider.model,
                       self.provider.datasource,
                       self.provider.transform,
                       self.provider.train], revise = False)

        self.push("classify",
                      [self.provider.transform_one,
                       self.provider.classify,
                       self.provider.inverse_transform_one], revise = False)

        self.push("update",
                      [self.provider.updatesource,
                       self.provider.transform,
                       self.provider.train,
                       ], revise = False)

        self.push("update_bulk",
                      [self.provider.updatesource_bulk,
                       self.provider.transform,
                       self.provider.train,
                       ], revise = False)
        
        self.deserialize()

    def train(self):
        return self.run("train")

    def classify(self, args):
        return self.run("classify", args)

    def update(self, args):
        return self.run("update", args)

    def update_bulk(self, args):
        return self.run("update_bulk", args)
