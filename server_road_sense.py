import time
import asyncio
import json
import uuid
import traceback
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import aiosqlite
from aiohttp import web
import logging
from datetime import datetime, timedelta
import os
import folium
import pandas as pd
from dateutil import parser
import math
from dotenv import load_dotenv
import heapq

EARTH_RADIUS=6372795
DISTANCE_BETWEN_POSITIONS=15

load_dotenv()
DATABASE_NAME = os.getenv("DATABASE_NAME")
PORT = os.getenv("PORT")
SENDER_EMAIL =  os.getenv("SENDER_EMAIL")
RECEIVER_EMAIL =  os.getenv("RECEIVER_EMAIL")
PASS_EMAIL =  os.getenv("PASS_EMAIL")

# Настройка системы логирования
logging.basicConfig(level=logging.INFO,
                    format = "%(asctime)s - %(module)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s")

# Создание объекта логгера
logger = logging.getLogger(__name__)
# Создание обработчика для вывода сообщений на экран
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
# Создание обработчика для записи сообщений в файл
file_handler = logging.FileHandler('server_log.log')
file_handler.setLevel(logging.DEBUG)
# Формат сообщений для файла
file_formatter = logging.Formatter("%(asctime)s - %(module)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s")
file_handler.setFormatter(file_formatter)
# Добавление обработчиков к логгеру
logger.addHandler(console_handler)
logger.addHandler(file_handler)

def calculateTheDistance (lat11,lon11, lat21, lon21):
    lat1 = lat11 * math.pi / 180
    lat2 = lat21 * math.pi / 180
    lon1 = lon11 * math.pi / 180
    lon2 = lon21 * math.pi / 180

    cl1 = math.cos(lat1)
    cl2 =  math.cos(lat2)
    sl1 =  math.sin(lat1)
    sl2 =  math.sin(lat2)
    delta = lon2 - lon1
    cdelta =  math.cos(delta)
    sdelta =  math.sin(delta)

    y =  math.sqrt( math.pow(cl2 * sdelta, 2) +  math.pow(cl1 * sl2 - sl1 * cl2 * cdelta, 2))
    x = sl1 * sl2 + cl1 * cl2 * cdelta

    ad =  math.atan2(y, x)
    dist = ad * EARTH_RADIUS;

    return dist

def calculateBearing(lat11, lon11, lat21, lon21):
    lat1 = lat11 * math.pi / 180
    lon1 = lon11 * math.pi / 180
    lat2 = lat21 * math.pi / 180
    lon2 = lon21 * math.pi / 180

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    bearing = math.atan2(math.sin(dlon) * math.cos(lat2),
                         math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon))

    return bearing * 180 / math.pi

def calculateLatLon2(lat1, lon1, distance, bearing):
    """
    Calculate the second coordinate given the first coordinate, distance, and bearing.

    Args:
        lat1 (float): Latitude of the first coordinate in degrees.
        lon1 (float): Longitude of the first coordinate in degrees.
        distance (float): Distance between the two coordinates in kilometers.
        bearing (float): Bearing from the first coordinate to the second coordinate in degrees.

    Returns:
        tuple(float, float): Latitude and longitude of the second coordinate in degrees.
    """

    lat1 = lat1 * math.pi / 180
    lon1 = lon1 * math.pi / 180
    bearing = bearing * math.pi / 180

    lat2 = math.asin(
        math.sin(lat1) * math.cos(distance / EARTH_RADIUS) +
        math.cos(lat1) * math.sin(distance / EARTH_RADIUS) * math.cos(bearing)
    )

    lon2 = lon1 + math.atan2(
        math.sin(bearing) * math.sin(distance / EARTH_RADIUS) * math.cos(lat1),
        math.cos(distance / EARTH_RADIUS) - math.sin(lat1) * math.sin(lat2)
    )

    return lat2 * 180 / math.pi, lon2 * 180 / math.pi

async def send_email(subject, body):
    sender_email = SENDER_EMAIL
    receiver_email = RECEIVER_EMAIL
    password = PASS_EMAIL

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))

    server = smtplib.SMTP("smtp.yandex.ru", 587)
    server.starttls()
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email, message.as_string())
    server.quit()

async def send_start_notification():
    subject = "Сервер запущен"
    body = "Ваш сервер успешно запущен и работает."
    await send_email(subject, body)

async def send_error_notification(error_message):
    subject = "Ошибка на сервере"
    body = f"Произошла ошибка на сервере:\n\n{error_message}"
    await send_email(subject, body)

async def send_shutdown_notification():
    subject = "Сервер остановлен"
    body = "Ваш сервер был остановлен."
    await send_email(subject, body)

async def handle_post(request):
    content = await request.text()
    json_data = json.loads(content)

    async with aiosqlite.connect(DATABASE_NAME) as conn:
        c = await conn.cursor()

        if 'id_user' in json_data:
            id_user = json_data['id_user']
            data = json_data['data']

            # Create a table for the user if it doesn't exist
            table_name = "user_" + id_user.replace("-", "_")

            # Check if the table for this device exists, if not, create it
            await c.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} ( date text, latitude real, longitude real, speed real, x_r real, y_r real, z_r real, x_l real, y_l real, z_l real)")

            data_lines = data.split('\n')

            # Insert the data into the table
            for line in data_lines:
                values = line.split(';')
                if len(values) == 10:
                    await c.execute(
                        f"INSERT INTO {table_name} (date, latitude, longitude, speed, x_r, y_r, z_r, x_l, y_l, z_l) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (values[0],
                         float(values[1]),
                         float(values[2]),
                         float(values[3]),
                         float(values[4]),
                         float(values[5]),
                         float(values[6]),
                         float(values[7]),
                         float(values[8]),
                         float(values[9]),)
                    )
            await conn.commit()

            logger.info(f"Data saved for user: {id_user}")
            return web.Response(status=202)

        elif 'email' in json_data:
            email = json_data['email']

            await c.execute("SELECT id_user FROM users WHERE email = ?", (email,))
            row = await c.fetchone()

            if row is not None:
                return web.Response(status=409)

            # Create a new user
            id_user = str(uuid.uuid4())
            current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            await c.execute("INSERT INTO users (email, id_user, date) VALUES (?, ?, ?)",
                                (email, id_user, current_datetime))

            # Commit the changes to the database
            await conn.commit()

            logger.info(f"New user registered with email: {email}, id_user: {id_user}")
            return web.json_response({'id_user': id_user}, status=200)


        elif 'hole' in json_data:
            await c.execute("SELECT * FROM hole WHERE activity >= 2")
            row = await c.fetchall()
            response_data = {"hole": row}
            logger.info(f"send hole")
            return web.json_response(response_data, status=200)

        else:
            # Bad Request
            return web.Response(status=400)

    return web.Response(status=200)

async def handle_get(request):
    return web.Response(status=403)


async def create_table():
    async with aiosqlite.connect(DATABASE_NAME) as conn:
        c = await conn.cursor()
        await c.execute("CREATE TABLE IF NOT EXISTS users (email text, id_user text, date text, UNIQUE (email, id_user))")
        await c.execute(
            "CREATE TABLE IF NOT EXISTS hole (date text, id_user text, latitude real, longitude real, speed real, value_hole real, activity integer)")
        await c.execute("CREATE TABLE IF NOT EXISTS users_rows (id_user text, row integer, UNIQUE (id_user))")

    logger.info(f"create table ok")


# добавление ям в таблицу hole и поиск похожих ям
async def add_hole_table(date, id_user, latitude, longitude, speed, value_hole):
    async with aiosqlite.connect(DATABASE_NAME) as conn:
        c = await conn.cursor()
        date_str = date.strftime('%Y-%m-%d %H:%M:%S.%f')

        # проверка есить ли в тб пожая яма?
        await c.execute("SELECT ROWID, * FROM hole WHERE date=? and id_user=? and latitude=? and longitude=?", (date_str,id_user,latitude,longitude,))
        existing_hole = await c.fetchone()
        if existing_hole:
            return
        else:
            await c.execute("SELECT ROWID, * FROM hole")
            existing_holes = await c.fetchall()

            if existing_holes:
                hole_dict = {}
                for item_row in existing_holes:
                    distance_points = calculateTheDistance(latitude, longitude, item_row[3], item_row[4])
                    hole_dict[distance_points]=item_row

                # Find the nearest hole
                min_key = min(hole_dict.keys())
                min_element = hole_dict[min_key]

                if min_key < DISTANCE_BETWEN_POSITIONS:
                    date_db_str = datetime.strptime(str(min_element[1]), '%Y-%m-%d %H:%M:%S.%f')

                    unique_user_ids_list = list(min_element[2].split('; '))
                    bool_user = id_user in unique_user_ids_list

                    if (id_user == min_element[2] or bool_user) and abs(date - date_db_str) < timedelta(minutes=15):
                        await c.execute(
                            "UPDATE hole SET latitude = ?, longitude = ?, value_hole = ?, activity = ?, id_user=? WHERE ROWID = ?",
                            (
                                (latitude + min_element[3]) / 2,
                                (longitude + min_element[4]) / 2,
                                (min_element[5] + value_hole) / 2,
                                1,
                                id_user if id_user == min_element[2] else min_element[2], min_element[0],
                            )
                        )

                    else:
                        await c.execute(
                            "UPDATE hole SET latitude = ?, longitude = ?, value_hole = ?, activity = ?, id_user=? WHERE ROWID = ?",
                            (
                                (latitude + min_element[3]) / 2,
                                (longitude + min_element[4]) / 2,
                                (min_element[5] + value_hole) / 2,
                                min_element[7] + 1,
                                min_element[2] if bool_user else min_element[2] + "; " + id_user,
                                min_element[0],
                            )
                        )

                else:
                    await c.execute(
                            "INSERT INTO hole (date, id_user, latitude, longitude, speed, value_hole, activity) VALUES (?, ?, ?,?,?,?,?)",
                            (date_str, id_user, latitude, longitude, speed, value_hole, 1,))
            else:
                await c.execute(
                    "INSERT INTO hole (date, id_user, latitude, longitude, speed, value_hole, activity) VALUES (?, ?, ?,?,?,?,?)",
                    (date_str, id_user, latitude, longitude, speed, value_hole, 1,))
        await conn.commit()

# создание карты с ямами
async def create_map_hole():
    async with aiosqlite.connect(DATABASE_NAME) as conn:
        c = await conn.cursor()
        await c.execute("SELECT latitude, longitude, speed, value_hole FROM hole")
        data = await c.fetchall()

        if not data:
            return

        # Создание карты
        m = folium.Map(location=[data[0][0], data[0][1]], zoom_start=10)
        for data_item in data:
            folium.Marker(location=[data_item[0], data_item[1]],
                              popup=data_item[3], icon=folium.Icon(color='black')).add_to(m)

        # Сохраняем карту ям в директорию
        try:
            m.save(f"map_hole.html")
            logger.info("Сохранена карта map_hole.html")
        except Exception as e:
            logger.info(f"Ошибка при сохранении карты map_hole.html: {e}")
            await send_error_notification(f"Ошибка при сохранении карты map_hole.html: {e}")

async def reseah_hole(num=2):
    """
    Асинхронный поиск ям по всем пользователям
    :param num: глубина ямы.
    """
    async with aiosqlite.connect(DATABASE_NAME) as conn:
        c = await conn.cursor()

        # получаем список таблиц
        await c.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in await c.fetchall()]
        if tables is None:
            return

        # Фильтрация таблиц по условию начала имени с 'user_'
        user_tables = [table for table in tables if table.startswith('user_')]
        if not user_tables:
            return

        for user_table in user_tables:
            user_id = user_table[len('user_'):].replace("_", "-")

            # Создаем каталог, если он не существует
            os.makedirs(f"maps_users/{user_id}", exist_ok=True)

            # Получаем данные из таблицы пользователя
            await c.execute(f"SELECT * FROM {user_table}")
            data_user_tables_item = await c.fetchall()
            data_user_table = pd.DataFrame(data_user_tables_item)

            # Получаем значение строки, до которой в последний раз обрабатывалось
            await c.execute("SELECT row FROM users_rows WHERE id_user = ?", (user_id,))
            row_table = await c.fetchone()

            if row_table is not None:
                row = row_table[0]
            else:
                row = 0
                await c.execute("INSERT INTO users_rows (id_user, row) VALUES(?,?)", (user_id, row,))
                await conn.commit()

            # Фильтрация и преобразование данных
            data_user_table = data_user_table[row:]
            if len(data_user_table) == 0:
                continue

            # Записываем значение последней строки, с которой в следующий раз будет обработка
            await c.execute("UPDATE users_rows SET row = ? WHERE id_user = ?", (len(data_user_table), user_id,))
            await conn.commit()

            # Преобразование столбца 'date' в формат datetime
            data_user_table[0] = data_user_table[0].apply(lambda x: parser.parse(x))

            # Определение границ разделения на основе разницы во времени более чем на 5 минут
            data_user_table = data_user_table.sort_values(0)
            diff = data_user_table[0].diff()
            boundary_indices = diff.index[diff > pd.Timedelta('5 min')]

            # Разделение данных на несколько частей
            data_chunks = []
            start = 0
            for end in boundary_indices:
                data_chunks.append(data_user_table.iloc[start:end])
                start = end
            data_chunks.append(data_user_table.iloc[start:])

            colors = ['blue', 'red', 'green', 'orange', 'purple']  # Вы можете добавить больше цветов, если нужно

            # Создание карты
            m = folium.Map(location=[data_user_table[1].iloc[-1], data_user_table[2].iloc[-1]], zoom_start=10)

            window_size = 30  # Window size for processing
            for i, item_chunk in enumerate(data_chunks):
                for itr in range(0, len(item_chunk), window_size):
                    # определяю доминирующую ось
                    chunk_range = item_chunk[[4, 5, 6]].iloc[itr:min(itr + window_size, len(item_chunk))]
                    max_absolute_acceleration = abs(chunk_range).max()
                    max_axis = max_absolute_acceleration.idxmax()

                    for itr_max_axis in range(0, len(chunk_range)):
                        if itr_max_axis + 1 < len(chunk_range):
                            value_diff = (chunk_range[max_axis].iloc[itr_max_axis] -
                                          chunk_range[max_axis].iloc[itr_max_axis + 1])

                            if abs(value_diff) >= num:
                                # нахожу точное значение gps
                                len_range = 1
                                hole_iter=0
                                iterator_start = itr_max_axis+itr
                                iterator_end = itr_max_axis + itr

                                while (iterator_start > 0 and
                                        item_chunk[1].iloc[iterator_start] == item_chunk[1].iloc[iterator_start - 1] and
                                        item_chunk[2].iloc[iterator_start] == item_chunk[2].iloc[iterator_start - 1]):
                                    iterator_start = iterator_start - 1

                                while (iterator_end < len(item_chunk[1]) - 1 and
                                        item_chunk[1].iloc[iterator_end] ==item_chunk[1].iloc[iterator_end + 1] and
                                        item_chunk[2].iloc[iterator_end] ==item_chunk[2].iloc[iterator_end + 1]):
                                    iterator_end = iterator_end + 1


                                len_range=iterator_end-iterator_start+1
                                hole_iter = itr_max_axis+itr - iterator_start

                                distance_points = calculateTheDistance(item_chunk[1].iloc[iterator_start], item_chunk[2].iloc[iterator_start],
                                                             item_chunk[1].iloc[iterator_end+1], item_chunk[2].iloc[iterator_end+1])

                                lantitude_hole,longitude_hole = calculateLatLon2(item_chunk[1].iloc[iterator_start], item_chunk[2].iloc[iterator_start],
                                                                                     (distance_points / len_range) * hole_iter,
                                                                                     calculateBearing(item_chunk[1].iloc[iterator_start], item_chunk[2].iloc[iterator_start],
                                                                                                      item_chunk[1].iloc[iterator_end+1], item_chunk[2].iloc[iterator_end+1]))

                                # добавляем маркер на карту
                                folium.Marker(location=[lantitude_hole,longitude_hole],
                                              popup=abs(value_diff), icon=folium.Icon(color='black')).add_to(m)

                                # добавление в таблицу hole ямы
                                try:
                                    await add_hole_table(date=item_chunk[0].iloc[itr_max_axis + itr],
                                                   id_user=user_id,
                                                   latitude=lantitude_hole,
                                                   longitude=longitude_hole,
                                                   speed=(item_chunk[3].iloc[iterator_start]+item_chunk[3].iloc[iterator_end+1])/2,
                                                   value_hole=abs(value_diff)
                                                   )

                                except Exception as e:
                                    logger.error(f"Ошибка при добавлении в таблицу hole: {e}")
                                    await send_error_notification(f"Ошибка при добавлении в таблицу hole: {e}")

                # рисуем проезд
                path_coordinates = list(zip(item_chunk[1], item_chunk[2]))
                color = colors[i % len(colors)]
                if len(path_coordinates) > 0:
                    folium.PolyLine(path_coordinates, color=color, tooltip="Coast").add_to(m)

            # Сохраняем карту пользователя в директорию
            try:
                m.save(f"maps_users/{user_id}/map_{user_table}_{datetime.now().strftime('%d.%m.%Y_%H.%M')}.html")
                logger.info(f"Сохранена карта {user_table}")
            except Exception as e:
                logger.error(f"Ошибка при сохранении карты: {e}")
                await send_error_notification(f"Ошибка при сохранении карты map_hole.html: {e}")


async def run_server(port=PORT):
    app = web.Application()
    app.router.add_post('/', handle_post)
    app.router.add_get('/', handle_get)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '', port=port).start()
    logger.info("Running server...")

async def main():
    await send_start_notification()
    await run_server()
    await create_table()

    while True:
        # Schedule the tasks to run every 120 and 240 seconds respectively
        await asyncio.sleep(60 * 60 * 4)  # 60*60
        asyncio.create_task(reseah_hole())
        await asyncio.sleep(60 * 60 * 6)  # 60*60*2
        asyncio.create_task(create_map_hole())

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(main())
        loop.run_forever()
    except Exception as e:
        error_message = traceback.format_exc()
        asyncio.run(send_error_notification(error_message))
    finally:
        logger.info('server stop')
        server_is_running = False
        asyncio.run(send_shutdown_notification())
