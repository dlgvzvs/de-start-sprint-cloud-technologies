from datetime import datetime
from logging import Logger
from uuid import UUID
from cdm_loader.repository import CdmRepository
from lib.kafka_connect import KafkaConsumer


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger,
                 ) -> None:

        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if msg is None: 
                break
            user_id = msg['payload']['user_id']
            products_list = msg['payload']['products']
            for product in products_list:
                product_id = product['product_id']
                product_name = product['product_name']
                category_id = product['category_id']
                category_name = product['category_name']
                self._cdm_repository.user_product_counters_upsert(user_id, product_id, product_name)
                self._cdm_repository.user_category_counters_upsert(user_id, category_id, category_name)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
