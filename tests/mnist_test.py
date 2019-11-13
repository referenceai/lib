from referenceai.dataset.images.mnist import MNISTImagesDataSource, MNIST
from PIL import Image
from random import randint
from loguru import logger

def test_mnist_dataset():
    dataset = MNISTImagesDataSource("mnist")
    dataset.load()

    idx = randint(0,len(dataset.test_labels))
    image = Image.fromarray(dataset.test_images[idx], 'L')
    label = dataset.test_labels[idx]
    image.save('/home/jrlomas/Desktop/{number}.png'.format(number=label))


def test_mnist_pipeline():
    mnist = MNIST()
    ds = mnist.ds
    test_image = ds.test_set()[0][1]
    test_label = ds.test_set()[1][1]

    mnist.train()
    mnist.train()
    char_number = mnist.classify(test_image)
    char_number = mnist.classify(test_image)
    assert(char_number == test_label)
    mnist.serialize()