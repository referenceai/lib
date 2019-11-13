from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gzip
import os
import shutil
import tempfile

import keras

import numpy as np
from six.moves import urllib
import tensorflow as tf
from abc import abstractmethod
from referenceai.transform import Transform
from referenceai.pipeline import PipelineAIProvider, PipelineAI
from referenceai.datasource import ImagesDataSource, ImageScheme, DataSource
from referenceai.model.images.lenet5 import LeNet5
from loguru import logger
from sklearn.preprocessing import OneHotEncoder

class MNISTImagesDataSource(ImagesDataSource):

    def __init__(self, id):
        super().__init__(id, ImageScheme.BINARY)
        self.train_images_filename = '{id}_images_file'.format(id=id)
        self.train_labels_filename = '{id}_labels_file'.format(id=id)
        self.test_images_filename = '{id}_test_file'.format(id=id)
        self.test_labels_filename = '{id}_labels_file'.format(id=id)

    def __read32(self,bytestream):
        """Read 4 bytes from bytestream as an unsigned 32-bit integer."""
        dt = np.dtype(np.uint32).newbyteorder('>')
        return np.frombuffer(bytestream.read(4), dtype=dt)[0]


    def __check_image_file_header(self, filename):
        """Validate that filename corresponds to images for the MNIST dataset."""
        with tf.gfile.Open(filename, 'rb') as f:
            magic = self.__read32(f)
            self.__read32(f)  # num_images, unused
            rows = self.__read32(f)
            cols = self.__read32(f)
            if magic != 2051:
                raise ValueError('Invalid magic number %d in MNIST file %s' % (magic,
                                                                                f.name))
            if rows != 28 or cols != 28:
                raise ValueError(
                    'Invalid MNIST file %s: Expected 28x28 images, found %dx%d' %
                    (f.name, rows, cols))


    def __check_labels_file_header(self, filename):
        """Validate that filename corresponds to labels for the MNIST dataset."""
        with tf.gfile.Open(filename, 'rb') as f:
            magic = self.__read32(f)
            self.__read32(f)  # num_items, unused
            if magic != 2049:
                raise ValueError('Invalid magic number %d in MNIST file %s' % (magic,
                                                                            f.name))


    def __download(self, directory, url, filename):
        """Download (and unzip) a file from the MNIST dataset if not already done."""
        filepath = os.path.join(directory, filename)
        if tf.gfile.Exists(filepath):
            return filepath
        if not tf.gfile.Exists(directory):
            tf.gfile.MakeDirs(directory)
        _, zipped_filepath = tempfile.mkstemp(suffix='.gz')
        print('Downloading %s to %s' % (url, zipped_filepath))
        urllib.request.urlretrieve(url, zipped_filepath)
        with gzip.open(zipped_filepath, 'rb') as f_in, \
            tf.gfile.Open(filepath, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(zipped_filepath)
        return filepath


    def dataset(self,directory, images_url, labels_url, images_file, labels_file):
        """Download and parse MNIST dataset."""

        images_file = self.__download(directory, images_url, images_file)
        labels_file = self.__download(directory, labels_url, labels_file)

        self.__check_image_file_header(images_file)
        self.__check_labels_file_header(labels_file)

        labels = np.fromfile(labels_file, dtype='ubyte', offset=8)
        images = np.fromfile(images_file, dtype='ubyte', offset=16)
        images = np.split(images, len(labels))
        def reshape2d(arr):
            return np.reshape(arr, (28,28))
        images = np.array(list(map(reshape2d,images)))
        
        return images, labels

    def __download_train_set(self, images_url, labels_url):
        """tf.data.Dataset object for MNIST training data."""
        self.train_images, self.train_labels = self.dataset(self.train_directory, images_url, labels_url, self.train_images_filename, self.train_labels_filename)

    def __download_test_set(self, images_url, labels_url):
        """tf.data.Dataset object for MNIST test data."""
        self.test_images, self.test_labels = self.dataset(self.test_directory, images_url, labels_url, self.test_images_filename, self.test_labels_filename)

    def load_from_url(self, train_url:(str,str), test_url:(str,str) = None):
        self.__download_train_set(train_url[0], train_url[1])
        self.__download_test_set(test_url[0], test_url[1])

    def load(self):
        base_url = 'https://storage.googleapis.com/cvdf-datasets/mnist/{filename}.gz'
        self.load_from_url(
            (base_url.format(filename = 'train-images-idx3-ubyte'),
             base_url.format(filename = 'train-labels-idx1-ubyte')
            ),

            test_url = (base_url.format(filename = 't10k-images-idx3-ubyte'),
             base_url.format(filename = 't10k-labels-idx1-ubyte'))
        )

    def train_set(self) -> (np.array, np.array):
        return self.train_images, self.train_labels

    def test_set(self) -> (np.array, np.array):
        return self.test_images, self.test_labels

class MNISTTransform(Transform):
    def __init__(self):
        self.encoder = OneHotEncoder()

    def fit(self, data: np.ndarray):
        self.encoder.fit(data)

    def forward_transform(self, data: np.ndarray) -> np.ndarray:
        return self.encoder.transform(data)

    def inverse_transform(self, data: np.ndarray) -> np.ndarray:
        return self.encoder.inverse_transform(data)
    

class MNISTPipelineAIProvider(PipelineAIProvider):

    def __init__(self, datasource: MNISTImagesDataSource):
        self.ds = datasource
        self.encoder = None

    def hyperparameters(self) -> dict:
        return {
            "epochs" : 5, 
            "batch_size": 128, 
            "optimizer" : 'sgd', 
            'loss' : 'binary_crossentropy'}

    def datasource(self) -> ImagesDataSource: 
        return self.ds

    def _transform_images(self, images: np.ndarray):
        return images.reshape(images.shape[0], 
                                            images.shape[1], 
                                            images.shape[2],
                                            1)

    def _transform_labels(self, labels: np.ndarray):
        pass

    def transform_one(self, image: np.ndarray) -> np.ndarray:
        r = self._transform_images(np.array([image]))
        return r

    def transform(self, ds: ImagesDataSource) -> (ImagesDataSource, Transform):
        train_images, train_image_labels = ds.train_set()
        test_images, test_image_labels = ds.test_set()
        
        train_images = self._transform_images(train_images)
        test_images = self._transform_images(test_images)
        
        test_image_labels = np.reshape(test_image_labels, (-1,1))
        train_image_labels = np.reshape(train_image_labels, (-1,1))

        # let's now take the categorical values and convert them to one-hot encoding
        t = MNISTTransform()
        t.encoder.fit(train_image_labels)
        
        train_image_labels_onehot = t.encoder.transform(train_image_labels).toarray()
        test_image_labels_onehot = t.encoder.transform(test_image_labels).toarray()
        ds.train_images = train_images
        ds.train_labels = train_image_labels_onehot
        ds.test_images = test_images
        ds.test_labels = test_image_labels_onehot

        return ds, t
    
    def inverse_transform_one(self, labels : np.ndarray, t : Transform) -> int:
        labels = labels[0]
        idx = labels.tolist().index(max(labels))
        labels = [0]*len(labels)
        labels[idx] = 1
        return int(t.encoder.inverse_transform([labels])[0][0])

    def model(self) -> keras.Model:
        # 2d 28*28 for MNIST dataset
        return LeNet5.model((28,28))

    def train(self, ds: ImagesDataSource, 
                    model: keras.Model,  
                    hyperparameters: dict,
                    transform: Transform) -> (keras.Model, Transform):
                    
        return LeNet5.train(model, ds, hyperparameters), transform

    def save(self, model: keras.Model):
        return model.save("mnist_lenet5.hd5")

    def load(self, model: keras.Model) -> keras.Model:
        return keras.models.load_model("mnist_lenet5.hd5")

    def classify(self, model: keras.Model, image: np.ndarray) -> np.ndarray:
        prediction = model.predict(image)
        return prediction

    def updatesource(self, ds: ImagesDataSource, image: np.ndarray, classification: str) -> ImagesDataSource:
        # TODO: Update the images data source with new image
        return ds

    def updatesource_bulk(self, ds: ImagesDataSource, images: np.ndarray, classifications: [str]) -> ImagesDataSource:
        # TODO: Update the images data source with new images
        return ds

class MNIST(PipelineAI):
    def __init__(self):
        self.ds = MNISTImagesDataSource("mnist")
        self.ds.load()
        super().__init__("mnist", MNISTPipelineAIProvider(self.ds))