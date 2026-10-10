from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:

        self._logger = logger

        self._batch_size = batch_size

        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository


    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            self._logger.info(f"Got message: {msg}")
            if msg is None: 
                break
            
            payload = msg['payload']

            # пользователи
            user_id = payload['user']['id']
            username = payload['user']['name']
            userlogin = username # как временная мера
            h_user_pk = self._dds_repository.h_user_insert(user_id)
            self._dds_repository.s_user_names_upsert(h_user_pk, username, userlogin)

            # рестораны
            restaurant_id = payload['restaurant']['id']
            restaurant_name = payload['restaurant']['name']
            h_restaurant_pk = self._dds_repository.h_restaurant_insert(restaurant_id)
            self._dds_repository.s_restaurant_names_upsert(h_restaurant_pk, restaurant_name)

            # заказы
            order_id = payload['id']
            order_dt = payload['date']
            cost = payload['cost']
            payment = payload['payment']
            status = payload['status']
            h_order_pk = self._dds_repository.h_order_insert(order_id, order_dt)
            self._dds_repository.s_order_cost_upsert(h_order_pk, cost, payment)
            self._dds_repository.s_order_status_upsert(h_order_pk, status)

            # продукты
            product_list = payload['products'] # на всякий случай беру список продуктов как в stg
            
            products_for_cdm = []
            self._dds_repository.l_order_user_insert(h_order_pk, h_user_pk)
            
            for product in product_list:
                product_id = product['id']
                product_name = product['name']
                category = product['category']
                h_product_pk = self._dds_repository.h_product_insert(product_id)
                h_category_pk = self._dds_repository.h_category_insert(category)
                self._dds_repository.s_product_names_upsert(h_product_pk, product_name)

                # связка данных
                self._dds_repository.l_product_category_insert(h_product_pk, h_category_pk)
                self._dds_repository.l_product_restaurant_insert(h_product_pk, h_restaurant_pk)
                self._dds_repository.l_order_product_insert(h_order_pk, h_product_pk)

                product_for_cdm = {
                    'product_id' : product_id,
                    'product_name' : product_name,
                    'category_id' : h_category_pk,
                    'category_name' : category
                }
                products_for_cdm.append(product_for_cdm)

            # выходное сообщение
            output_message = {
                'object_id' : msg['object_id'],
                'object_type' : msg['object_type'],
                'payload' : { # ключи проверить 
                    "user_id": user_id,
                    "order_id": order_id,
                    "products": products_for_cdm
                }
            }
            self._producer.produce(output_message)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
