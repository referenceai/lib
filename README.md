# reference-ai/lib
The easiest way in python to run, deploy, and share AI models

### 1. Implement an PipelineAIProvider, or use one already provided
```python
from referenceai.dataset.images.MNIST import MNISTPipelineAIProvider
provider = MNISTPipelineAIProvider()
```

### 2. Create a new PipelineAI
```python
from referenceai.pipeline import PipelineAI
p = PipelineAI("my_provider", provider)
```

### 3. You can now train, classify, and update your model effortlessly
```python
p.train()
p.classify(image)
p.update(new_image)
p.update_bulk([new_image_1, new_image_2])
```

### 4. Serve your model over RESTful or GraphQL interfaces
```python
from referenceai.servers import GraphQLServer, RESTServer
g_server = GraphQLServer(p)
r_server = RESTServer(p)

g_server.run(port = 8080)
r_server.run(posrt = 3000)
```
