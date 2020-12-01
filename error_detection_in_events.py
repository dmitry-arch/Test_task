import os
import sys
import json
import jsonschema


# Указываем пути к файлам
DATA_DIR = os.getcwd() + '/task_folder/'

file_path = os.path.join(DATA_DIR, 'event')
schema_path = os.path.join(DATA_DIR, 'schema')

# Создаём списки с названиями json файлов и схем
names_json_file = os.listdir(file_path)
names_schema_file = os.listdir(schema_path)
names_schema = [i.split('.')[0] for i in names_schema_file]


def get_data(path, names_file):
    """
    Загружаем json файлы и добавляем их в словарь с ключём по названию
    path - путь к папке с файлами
    names_file - список с названиями файлов

    """
    all_data = {}
    for name_json_file in names_file:
        one_file = os.path.join(path, name_json_file)
        with open(one_file, 'r') as f:

            json_file = json.loads(f.read())
            all_data[f'{name_json_file}'] = json_file

    return all_data


def check_dict_keys(dict_data):
    """
    Проверка на наличие всех ключей
    dict_data - словарь для проверки
    return - список отсутствующих ключей
    """

    loss_keys = []

    validate_dict_keys = ['id', 'user_id', 'event', 'data', 'created_at', 'environment_id']
    got_dict_keys = list(dict_data.keys())
    if got_dict_keys == validate_dict_keys:
        return loss_keys
    else:
        for dict_key in validate_dict_keys:
            if dict_key not in got_dict_keys:
                loss_keys.append(dict_key)
    return loss_keys


# Получаем словари со всеми данными
all_json_data = get_data(file_path, names_json_file)
all_schema_data = get_data(schema_path, names_schema_file)

# Получаем список с ключами для данных
keys_json = list(all_json_data.keys())

# Создаём словарь с ключами из имён файлов для записи ошибок в логи.
LOGS_DATA = {}
for name in list(keys_json):
    LOGS_DATA[name] = []


# Проверка на None и на отсутствие данных
drop_files = []
for index, name_file in enumerate(keys_json):
    if all_json_data[name_file] is None:
        LOGS_DATA[name_file].append(['Полностью отсутствуют даные.', 'Добавьте информацию.'])
        drop_files.append(name_file)

    elif len(all_json_data[name_file]) == 0:
        LOGS_DATA[name_file].append(['Отсутствуют ключи и значения.', 'Добавьте информацию.'])
        drop_files.append(name_file)

# Убираем пустые файлы из списка для дальнейшей проверки
for drop_file in drop_files:
    keys_json.remove(drop_file)

# Проверка на наличие всех ключей
for name_file in keys_json:
    list_of_keys = check_dict_keys(all_json_data[name_file])
    if len(list_of_keys) != 0:
        LOGS_DATA[name_file].append([f"Отсутствуют ключи: {list_of_keys}",
                                     f'Добавьте информацию в поля: {list_of_keys}'])

# Проверка на соответствие схеме
for count, name_file in enumerate(keys_json):

    one_json_data = all_json_data[name_file]
    event = one_json_data['event']
    event = event.split(' ')

    if len(event) > 1:

        log_info_event = one_json_data['event']
        LOGS_DATA[name_file].append(
            ['В названии события присутствуют пробелы', f'Уберите пробелы в событии - {log_info_event}'])

        event = event[0] + event[-1]
    else:
        event = event[0]

    try:
        schema = all_schema_data[event + '.schema']
    except KeyError:

        LOGS_DATA[name_file].append(
            [f'Нет схемы для события {event}', f'Добавьте схему для обработки события - {event}'])
        continue

    try:
        jsonschema.validate(instance=one_json_data['data'], schema=schema)
    except jsonschema.exceptions.ValidationError:

        error = sys.exc_info()
        LOGS_DATA[name_file].append([str(error[1]).split('\n')[0], 'Добавьте отсутствующую информацию'])

# Сохранение файла с логами
with open('logs_file.txt', 'w') as f:
    for key, values in LOGS_DATA.items():
        if not values:
            continue
        else:
            f.write(f'В файле {key} присутствют такие ошибки:\n\n')
            for num, value in enumerate(values):
                f.write(f'  {num + 1}. {value[0]}\n')
                f.write(f'  Исправление: {value[1]}\n\n')


