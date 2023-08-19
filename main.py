import psycopg2


# Функция, создающая структуру БД (таблицы)
def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS clients(
            id_client SERIAL PRIMARY KEY,
            first_name VARCHAR(45) NOT NULL,
            last_name VARCHAR(75) NOT NULL,
            email VARCHAR(250) NOT NULL 
            );
            """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS phone(
            id SERIAL PRIMARY KEY,
            id_client INTEGER NOT NULL REFERENCES clients(id_client),
            phone_number VARCHAR(11) UNIQUE
            );
            """)
    print('Создана БД')
    conn.commit()


# Поиск id клиента
def get_id_of_client(conn, first_name, last_name):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id_client FROM clients WHERE first_name=%s AND last_name=%s 
        """, (first_name, last_name,))
        client_id = cur.fetchone()[0]
    return client_id


# Функция, позволяющая добавить нового клиента
def add_client(conn, first_name, last_name, email, phone_number=None):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO  clients (first_name, last_name, email)
        VALUES (%s, %s, %s);
        """, (first_name, last_name, email,))
        if phone_number:
            client_id = get_id_of_client(conn, first_name, last_name)
            cur.execute("""
                    INSERT INTO  phone (id_client, phone_number)
                    VALUES (%s, %s) ;
                    """, (client_id, phone_number,))
    conn.commit()


# Функция, позволяющая добавить телефон для существующего клиента
def add_phone(conn, client_id, phone_number):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO  phone (id_client, phone_number)
        VALUES (%s, %s)
        """, (client_id, phone_number,))
    conn.commit()


# Функция, позволяющая изменить данные о клиенте.
def change_client(conn, client_id, first_name=None, last_name=None, email=None, phone_number=None):
    with conn.cursor() as cur:
        if first_name:
            cur.execute("""
            UPDATE clients SET first_name=%s WHERE id_client=%s
            """, (first_name, client_id,))
        if last_name:
            cur.execute("""
            UPDATE clients SET last_name=%s WHERE id_client=%s
            """, (last_name, client_id,))
        if email:
            cur.execute("""
            UPDATE clients SET email=%s WHERE id_client=%s
            """, (email, client_id,))
        if phone_number:
            cur.execute("""
            UPDATE phone SET phone_number=%s WHERE id_client=%s
            """, (phone_number, client_id,))
    conn.commit()


# Функция, позволяющая удалить телефон для существующего клиента.
def delete_phone(conn, client_id, phone_number):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE  FROM phone
        WHERE id_client=%s AND phone_number=%s
        """, (client_id, phone_number))
    conn.commit()


# Функция, позволяющая удалить существующего клиента
def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE  FROM phone
        WHERE id_client=%s 
        """, (client_id,))

        cur.execute("""
        DELETE  FROM clients
        WHERE id_client=%s 
        """, (client_id,))
    conn.commit()


# Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону.
def find_client(conn, first_name=None, last_name=None, email=None, phone_number=None):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT *
        FROM  clients AS c
        LEFT JOIN  phone  AS p ON c.id_client = p.id_client
        WHERE (first_name = %(first_name)s OR %(first_name)s IS NULL)
                   AND (last_name = %(last_name)s OR %(last_name)s IS NULL)
                   AND (email = %(email)s OR %(email)s IS NULL)
                   AND (phone_number = %(phone_number)s OR %(phone_number)s IS NULL);
            """, {"first_name": first_name, "last_name": last_name, "email": email, "phone_number": phone_number})
        return  cur.fetchall()


with psycopg2.connect(database="clients_information_14_08", user="postgres", password="Postgres") as conn:
    create_db(conn)
    print(add_client(conn, 'Alla', 'Arisova', 'alla@mail.ru', '81111111111'))
    print(add_client(conn, 'Viktor', 'Viktorov', 'viktor@mail.ru', '81111111122'))
    print(add_client(conn, 'Boris', 'Boovris', 'boris@mail.ru', '81111111133'))
    print(add_client(conn, 'Grigor', 'Grishin', 'grigor@mail.ru'))
    print(add_client(conn, 'Grigor', 'Goga', 'grigorgoga@mail.ru'))
    alla_id = get_id_of_client(conn, 'Alla', 'Arisova')
    bor_id = get_id_of_client(conn, 'Boris', 'Boovris')
    vik_id = get_id_of_client(conn, 'Viktor', 'Viktorov')
    gri_id = get_id_of_client(conn, 'Grigor', 'Grishin')
    print(add_phone(conn, alla_id, '81111111155'))
    print(add_phone(conn, bor_id, '81111111166'))
    print(add_phone(conn, gri_id, '81145645778'))
    print(change_client(conn, alla_id, first_name='Anna', last_name=None, email='allaalla@mail.ru', phone_number=None))
    print(change_client(conn, vik_id, first_name=None, last_name=None, email='vkviktor@mail.ru', phone_number='89666666666'))
    print(delete_phone(conn, vik_id, '81111111122'))
    print(delete_client(conn, gri_id))
    required_client_1 = find_client(conn, first_name=None, last_name='Viktorov', email=None, phone_number=None)
    print(required_client_1)
    required_client_2 = find_client(conn, first_name='Grigor', last_name=None, email='grigorgoga@mail.ru', phone_number=None)
    print(required_client_2)
