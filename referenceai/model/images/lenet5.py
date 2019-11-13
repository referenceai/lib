from keras import Model, Sequential, layers
from keras.optimizers import Optimizer
from referenceai.datasource import ImagesDataSource
import numpy as np

class LeNet5():
    @classmethod
    def model(cls, input_shape : (int,int)):
        model = Sequential()
        model.add(layers.Conv2D(filters=6, 
                                kernel_size=(3, 3), 
                                activation='relu', 
                                input_shape=(input_shape[0],input_shape[1],1)))
        model.add(layers.AveragePooling2D())
        model.add(layers.Conv2D(filters=16, 
                                kernel_size=(3, 3), 
                                activation='relu'))
        model.add(layers.AveragePooling2D())
        model.add(layers.Flatten())
        model.add(layers.Dense(units=120, activation='relu'))
        model.add(layers.Dense(units=84, activation='relu'))
        model.add(layers.Dense(units=10, activation = 'softmax'))
        return model

    @classmethod
    def train(cls, model: Model, datasource: ImagesDataSource, hyperparameters: dict) -> Model:
        model.compile(optimizer = hyperparameters['optimizer'], 
                      loss = hyperparameters['loss'])
        model.fit(datasource.train_set()[0], 
                  datasource.train_set()[1],
                  validation_data=(datasource.test_set()[0], 
                                   datasource.test_set()[1]),
                  epochs = hyperparameters["epochs"],
                  batch_size = hyperparameters["batch_size"])
        return model

    @classmethod
    def classify(cls, model: Model, image: np.ndarray) -> np.ndarray:
        return model.predict(image)