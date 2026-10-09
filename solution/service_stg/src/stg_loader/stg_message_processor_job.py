import time
from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer 
from lib.redis import RedisClient
from stg_loader.repository import StgRepository
import json

class StgMessageProcessor:
    def __init__(self,
                consumer: KafkaConsumer,
                producer: KafkaProducer,
                redis: RedisClient,
                stg_repository: StgRepository,
                batch_size: int,
                logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size
        self._logger = logger

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            
            if msg is None:
                break
            
            self._stg_repository.order_events_insert(
                object_id=msg['object_id'],
                object_type=msg['object_type'],
                sent_dttm=msg['sent_dttm'],
                payload=json.dumps(msg['payload'])
            )

            user_id = msg['payload']['user']['id']
            user = self._redis.get(user_id)

            restaurant_id = msg['payload']['restaurant']['id']
            restaurant = self._redis.get(restaurant_id)

            order_items = msg['payload']['order_items']
            products_list = []
            for item in order_items:
                category = None
                for menu_item in restaurant['menu']:
                    if menu_item['_id'] == item['id']:
                        category = menu_item['category']
                        break

                current_product = {
                    'id' : item['id'],
                    'name' : item['name'],
                    'price' : item['price'],
                    'quantity' : item['quantity'],
                    'category' : category
                }
                products_list.append(current_product)

            output_message = {
                'object_id': msg['object_id'],
                'object_type': msg['object_type'],
                'payload': {
                    "id": msg['payload']['id'],
                    "date": msg['payload']['date'],
                    "cost": msg['payload']['cost'],
                    "payment": msg['payload']['payment'],
                    "status": msg['payload']['final_status'],
                    "restaurant": {
                        "id": msg['payload']['restaurant']['id'],
                        "name": restaurant['name']
                    },
                    "user": {
                        "id": msg['payload']['user']['id'],
                        "name": user['name']
                    },
                    "products": products_list
                }
            }

            self._producer.produce(output_message)
 
        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")
