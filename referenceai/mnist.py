from abc import abstractmethod
from .pipeline import PipelineAIProvider, ImagesDataSource

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gzip
import os
import shutil
import tempfile

import numpy as np
from six.moves import urllib
import tensorflow as tf

class MNISTImagesDataSource(ImagesDataSource):

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


    def __download(self, directory, filename):
        """Download (and unzip) a file from the MNIST dataset if not already done."""
        filepath = os.path.join(directory, filename)
        if tf.gfile.Exists(filepath):
            return filepath
        if not tf.gfile.Exists(directory):
            tf.gfile.MakeDirs(directory)
        # CVDF mirror of http://yann.lecun.com/exdb/mnist/
        url = 'https://storage.googleapis.com/cvdf-datasets/mnist/' + filename + '.gz'
        _, zipped_filepath = tempfile.mkstemp(suffix='.gz')
        print('Downloading %s to %s' % (url, zipped_filepath))
        urllib.request.urlretrieve(url, zipped_filepath)
        with gzip.open(zipped_filepath, 'rb') as f_in, \
            tf.gfile.Open(filepath, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(zipped_filepath)
        return filepath


    def dataset(self,directory, images_file, labels_file):
        """Download and parse MNIST dataset."""

        images_file = self.__download(directory, images_file)
        labels_file = self.__download(directory, labels_file)

        self.__check_image_file_header(images_file)
        self.__check_labels_file_header(labels_file)

        def decode_image(image):
            # Normalize from [0, 255] to [0.0, 1.0]
            image = tf.decode_raw(image, tf.uint8)
            image = tf.cast(image, tf.float32)
            image = tf.reshape(image, [28*28])
            return image / 255.0

        def decode_label(label):
            label = tf.decode_raw(label, tf.uint8)  # tf.string -> [tf.uint8]
            label = tf.reshape(label, [])  # label is a scalar
            return tf.cast(label, tf.int32)

        images = tf.data.FixedLengthRecordDataset(
            images_file, 28 * 28, header_bytes=16).map(decode_image)
        labels = tf.data.FixedLengthRecordDataset(
            labels_file, 1, header_bytes=8).map(decode_label)
        return tf.data.Dataset.zip((images, labels))


    def train(self, directory):
        """tf.data.Dataset object for MNIST training data."""
        return self.dataset(directory, 'train-images-idx3-ubyte',
                        'train-labels-idx1-ubyte')


    def test(self, directory):
        """tf.data.Dataset object for MNIST test data."""
        return self.dataset(directory, 't10k-images-idx3-ubyte', 't10k-labels-idx1-ubyte')


    def load(self, directory, images_file, labels_file):
        """Download and parse MNIST dataset."""

        images_file = self.__download(directory, images_file)
        labels_file = self.__download(directory, labels_file)

        self.__check_image_file_header(images_file)
        self.__check_labels_file_header(labels_file)

        def decode_image(image):
            # Normalize from [0, 255] to [0.0, 1.0]
            image = tf.decode_raw(image, tf.uint8)
            image = tf.cast(image, tf.float32)
            image = tf.reshape(image, [784])
            return image / 255.0

        def decode_label(label):
            label = tf.decode_raw(label, tf.uint8)  # tf.string -> [tf.uint8]
            label = tf.reshape(label, [])  # label is a scalar
            return tf.cast(label, tf.int32)

        images = tf.data.FixedLengthRecordDataset(
            images_file, 28 * 28, header_bytes=16).map(decode_image)
        labels = tf.data.FixedLengthRecordDataset(
            labels_file, 1, header_bytes=8).map(decode_label)
        return tf.data.Dataset.zip((images, labels))

class MNIST(PipelineAIProvider):
    def hyperparameters(self):
        pass

    def optimizer(self):
        pass

    def train_datasource(self):
        def r() -> MNISTImagesDataSource:
            pass
        return r

    def test_datasource(self):
        pass
    def transform(self):
        pass

    def train(self):
        pass

    def save(self):
        pass

    def load(self):
        pass