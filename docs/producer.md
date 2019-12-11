
```python
from core.model import BaseTopicSchema
from core.core import ProducerComponent
from core.model import topic_name



class City(BaseTopicSchema):
    name: str


@topic_name("user-topic")
class User(BaseTopicSchema):
    name: str
    age: int
    city: City


producer = ProducerComponent(User, key_serializer=lambda key: key.encode())
await producer.start()
user = User(name='Onna', age=5, city=City(name='Durham'))
r = await producer.send(key=user.name, value=user)
```
