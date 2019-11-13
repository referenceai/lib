import numpy as np
from abc import abstractmethod

class Transform():

    @abstractmethod
    def fit(self, data: np.ndarray):
        pass

    @abstractmethod
    def forward_transform(self, data: np.ndarray) -> np.ndarray:
        pass

    @abstractmethod
    def inverse_transform(self, data: np.ndarray) -> np.ndarray:
        pass