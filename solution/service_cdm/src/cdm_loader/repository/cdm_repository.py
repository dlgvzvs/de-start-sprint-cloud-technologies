from lib.pg import PgConnect

class CdmRepository:
    def __init__(self, 
                 db: PgConnect) -> None:
        self._db = db

    def user_product_counters_upsert(self, user_id, product_id, product_name) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute (
                    """
                        SELECT user_id, product_id
                        FROM cdm.user_product_counters
                        WHERE user_id = %(user_id)s AND product_id = %(product_id)s
                    """,
                    {'user_id': user_id, 
                     'product_id': product_id})
                row = cur.fetchone()
                if row:
                    cur.execute (
                    """ 
                        UPDATE cdm.user_product_counters
                        SET order_cnt = order_cnt + 1
                        WHERE user_id = %(user_id)s AND product_id = %(product_id)s
                    """,
                    {'user_id': user_id, 
                     'product_id': product_id})
                else:
                   cur.execute (
                    """
                        INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt)
                        VALUES (%(user_id)s, %(product_id)s, %(product_name)s, 1)
                    """,
                    {'user_id': user_id, 
                     'product_id': product_id,
                     'product_name': product_name})

    def user_category_counters_upsert(self, user_id, category_id, category_name) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute (
                    """
                        SELECT user_id, category_id
                        FROM cdm.user_category_counters
                        WHERE user_id = %(user_id)s AND category_id = %(category_id)s
                    """,
                    {'user_id': user_id, 
                     'category_id': category_id})
                row = cur.fetchone()
                if row:
                    cur.execute (
                    """ 
                        UPDATE cdm.user_category_counters
                        SET order_cnt = order_cnt + 1
                        WHERE user_id = %(user_id)s AND category_id = %(category_id)s
                    """,
                    {'user_id': user_id, 
                     'category_id': category_id})
                else:
                   cur.execute (
                    """
                        INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt)
                        VALUES (%(user_id)s, %(category_id)s, %(category_name)s, 1)
                    """,
                    {'user_id': user_id, 
                     'category_id': category_id,
                     'category_name': category_name})