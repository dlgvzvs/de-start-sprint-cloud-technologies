from lib.pg import PgConnect
from datetime import datetime

class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_user_insert (self,
    user_id: str) -> str:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute (
                    """
                        SELECT h_user_pk FROM dds.h_user WHERE user_id = %(user_id)s
                    """, {'user_id': user_id})
                row = cur.fetchone()
                if row:
                    h_user_pk = row[0]
                else:
                    cur.execute ("""
                        INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src)
                        VALUES (gen_random_uuid(), %(user_id)s, now(), 'orders-system-kafka')
                        RETURNING h_user_pk
                    """,{'user_id': user_id})
                    row = cur.fetchone()
                    h_user_pk = row[0]
                return h_user_pk


    def h_restaurant_insert  (self,
    restaurant_id: str) -> str:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute (
                    """
                        SELECT h_restaurant_pk FROM dds.h_restaurant WHERE restaurant_id = %(restaurant_id)s
                    """, {'restaurant_id': restaurant_id})
                row = cur.fetchone()
                if row:
                    h_restaurant_pk = row[0]
                else:
                    cur.execute ("""
                        INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
                        VALUES (gen_random_uuid(), %(restaurant_id)s, now(), 'orders-system-kafka')
                        RETURNING h_restaurant_pk
                    """,{'restaurant_id': restaurant_id})
                    row = cur.fetchone()
                    h_restaurant_pk = row[0]
                return h_restaurant_pk

    def h_order_insert  (self,
    order_id : str,
    order_dt: datetime) -> str:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute (
                    """
                        SELECT h_order_pk FROM dds.h_order WHERE order_id = %(order_id)s
                    """, {'order_id': order_id})
                row = cur.fetchone()
                if row:
                    h_order_pk = row[0]
                else:
                    cur.execute ("""
                        INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
                        VALUES (gen_random_uuid(), %(order_id)s, %(order_dt)s, now(), 'orders-system-kafka')
                        RETURNING h_order_pk
                    """,
                        {'order_id': order_id,
                        'order_dt': order_dt})
                    row = cur.fetchone()
                    h_order_pk = row[0]
                return h_order_pk

    def h_product_insert  (self,
    product_id: str) -> str:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute (
                    """
                        SELECT h_product_pk FROM dds.h_product WHERE product_id = %(product_id)s
                    """, {'product_id': product_id})
                row = cur.fetchone()
                if row:
                    h_product_pk = row[0]
                else:
                    cur.execute ("""
                        INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src)
                        VALUES (gen_random_uuid(), %(product_id)s, now(), 'orders-system-kafka')
                        RETURNING h_product_pk
                    """,{'product_id': product_id})
                    row = cur.fetchone()
                    h_product_pk = row[0]
                return h_product_pk

    def h_category_insert  (self,
    category_name: str) -> str:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute (
                    """
                        SELECT h_category_pk FROM dds.h_category WHERE category_name = %(category_name)s
                    """, {'category_name': category_name})
                row = cur.fetchone()
                if row:
                    h_category_pk = row[0]
                else:
                    cur.execute ("""
                        INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src)
                        VALUES (gen_random_uuid(), %(category_name)s, now(), 'orders-system-kafka')
                        RETURNING h_category_pk
                    """,{'category_name': category_name})
                    row = cur.fetchone()
                    h_category_pk = row[0]
                return h_category_pk

    def s_user_names_upsert (self, 
    h_user_pk: str, 
    username: str, 
    userlogin: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute (
                    """
                        SELECT h_user_pk FROM dds.s_user_names WHERE h_user_pk = %(h_user_pk)s
                    """, {'h_user_pk': h_user_pk})
                row = cur.fetchone()
                if row:
                    cur.execute(
                        """
                            UPDATE dds.s_user_names 
                            SET username = %(username)s, userlogin = %(userlogin)s, load_dt = now()
                            WHERE h_user_pk = %(h_user_pk)s
                        """, {'h_user_pk': h_user_pk, 
                            'username': username, 
                            'userlogin': userlogin}
                    )
                else:
                    cur.execute ("""
                        INSERT INTO dds.s_user_names (h_user_pk, username, userlogin, load_dt, load_src)
                        VALUES (%(h_user_pk)s, %(username)s, %(userlogin)s, now(), 'orders-system-kafka')
                    """, {'h_user_pk': h_user_pk, 
                        'username': username, 
                        'userlogin': userlogin})

    def s_product_names_upsert (self, 
    h_product_pk: str, 
    name: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute (
                    """
                        SELECT h_product_pk FROM dds.s_product_names WHERE h_product_pk = %(h_product_pk)s
                    """, {'h_product_pk': h_product_pk})
                row = cur.fetchone()
                if row:
                    cur.execute(
                        """
                            UPDATE dds.s_product_names 
                            SET name = %(name)s, load_dt = now()
                            WHERE h_product_pk = %(h_product_pk)s
                        """, {'h_product_pk': h_product_pk , 
                            'name': name}
                    )
                else:
                    cur.execute ("""
                        INSERT INTO dds.s_product_names (h_product_pk, name, load_dt, load_src)
                        VALUES (%(h_product_pk)s, %(name)s, now(), 'orders-system-kafka')
                    """, {'h_product_pk': h_product_pk, 
                        'name': name})

    def s_restaurant_names_upsert (self, 
    h_restaurant_pk: str, 
    name: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute (
                    """
                        SELECT h_restaurant_pk FROM dds.s_restaurant_names WHERE h_restaurant_pk = %(h_restaurant_pk)s
                    """, {'h_restaurant_pk': h_restaurant_pk})
                row = cur.fetchone()
                if row:
                    cur.execute(
                        """
                            UPDATE dds.s_restaurant_names 
                            SET name = %(name)s, load_dt = now()
                            WHERE h_restaurant_pk = %(h_restaurant_pk)s
                        """, {'h_restaurant_pk': h_restaurant_pk , 
                            'name': name}
                    )
                else:
                    cur.execute ("""
                        INSERT INTO dds.s_restaurant_names (h_restaurant_pk, name, load_dt, load_src)
                        VALUES (%(h_restaurant_pk)s, %(name)s, now(), 'orders-system-kafka')
                    """, {'h_restaurant_pk': h_restaurant_pk, 
                        'name': name})

    def s_order_cost_upsert (self, 
    h_order_pk: str, 
    cost: float,
    payment: float) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute (
                    """
                        SELECT h_order_pk FROM dds.s_order_cost WHERE h_order_pk = %(h_order_pk)s
                    """, {'h_order_pk': h_order_pk})
                row = cur.fetchone()
                if row:
                    cur.execute(
                        """
                            UPDATE dds.s_order_cost 
                            SET cost = %(cost)s, payment = %(payment)s, load_dt = now()
                            WHERE h_order_pk = %(h_order_pk)s
                        """, {'h_order_pk': h_order_pk, 
                            'cost': cost,
                            'payment': payment}
                    )
                else:
                    cur.execute ("""
                        INSERT INTO dds.s_order_cost (h_order_pk, cost, payment, load_dt, load_src)
                        VALUES (%(h_order_pk)s, %(cost)s, %(payment)s, now(), 'orders-system-kafka')
                    """, {'h_order_pk': h_order_pk, 
                            'cost': cost,
                            'payment': payment})

    def s_order_status_upsert (self, 
    h_order_pk: str, 
    status : str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute (
                    """
                        SELECT h_order_pk FROM dds.s_order_status WHERE h_order_pk = %(h_order_pk)s
                    """, {'h_order_pk': h_order_pk})
                row = cur.fetchone()
                if row:
                    cur.execute(
                        """
                            UPDATE dds.s_order_status 
                            SET status = %(status)s, load_dt = now()
                            WHERE h_order_pk = %(h_order_pk)s
                        """, {'h_order_pk': h_order_pk, 
                            'status': status}
                    )
                else:
                    cur.execute ("""
                        INSERT INTO dds.s_order_status (h_order_pk, status, load_dt, load_src)
                        VALUES (%(h_order_pk)s, %(status)s, now(), 'orders-system-kafka')
                    """, {'h_order_pk': h_order_pk, 
                            'status': status})

    def l_order_user_insert(self, 
    h_order_pk: str, 
    h_user_pk: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute (
                    """
                        SELECT hk_order_user_pk 
                        FROM dds.l_order_user 
                        WHERE h_order_pk = %(h_order_pk)s AND h_user_pk = %(h_user_pk)s
                    """,
                    {'h_order_pk': h_order_pk, 'h_user_pk': h_user_pk}
                    )
                row = cur.fetchone()
                if row:
                    pass
                else:
                    cur.execute ("""
                        INSERT INTO dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
                        VALUES (gen_random_uuid(), %(h_order_pk)s, %(h_user_pk)s, now(), 'orders-system-kafka')
                    """, {'h_order_pk': h_order_pk, 'h_user_pk': h_user_pk})

    def l_order_product_insert(self, 
    h_order_pk: str, 
    h_product_pk: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute (
                    """
                        SELECT hk_order_product_pk 
                        FROM dds.l_order_product 
                        WHERE h_order_pk = %(h_order_pk)s AND h_product_pk = %(h_product_pk)s
                    """,
                    {'h_order_pk': h_order_pk, 'h_product_pk': h_product_pk}
                    )
                row = cur.fetchone()
                if row:
                    pass
                else:
                    cur.execute ("""
                        INSERT INTO dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                        VALUES (gen_random_uuid(), %(h_order_pk)s, %(h_product_pk)s, now(), 'orders-system-kafka')
                    """, {'h_order_pk': h_order_pk, 'h_product_pk': h_product_pk})

    def l_product_category_insert(self, 
    h_category_pk: str, 
    h_product_pk: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute (
                    """
                        SELECT hk_product_category_pk 
                        FROM dds.l_product_category 
                        WHERE h_category_pk = %(h_category_pk)s AND h_product_pk = %(h_product_pk)s
                    """,
                    {'h_category_pk': h_category_pk, 'h_product_pk': h_product_pk}
                    )
                row = cur.fetchone()
                if row:
                    pass
                else:
                    cur.execute ("""
                        INSERT INTO dds.l_product_category (hk_product_category_pk, h_category_pk, h_product_pk, load_dt, load_src)
                        VALUES (gen_random_uuid(), %(h_category_pk)s, %(h_product_pk)s, now(), 'orders-system-kafka')
                    """, {'h_category_pk': h_category_pk, 'h_product_pk': h_product_pk})

    def l_product_restaurant_insert(self, 
    h_restaurant_pk: str, 
    h_product_pk: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute (
                    """
                        SELECT hk_product_restaurant_pk 
                        FROM dds.l_product_restaurant 
                        WHERE h_restaurant_pk = %(h_restaurant_pk)s AND h_product_pk = %(h_product_pk)s
                    """,
                    {'h_restaurant_pk': h_restaurant_pk, 'h_product_pk': h_product_pk}
                    )
                row = cur.fetchone()
                if row:
                    pass
                else:
                    cur.execute ("""
                        INSERT INTO dds.l_product_restaurant (hk_product_restaurant_pk, h_restaurant_pk, h_product_pk, load_dt, load_src)
                        VALUES (gen_random_uuid(), %(h_restaurant_pk)s, %(h_product_pk)s, now(), 'orders-system-kafka')
                    """, {'h_restaurant_pk': h_restaurant_pk, 'h_product_pk': h_product_pk})