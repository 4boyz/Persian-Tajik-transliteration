from typing import Any
import gzip
import os.path
import os

ID_COLUMN = 'id'
CONTENT_COLUMN = 'Content'
STATUS_CODE_COLUMN = 'StatusCode'

class DumpReader:
    def __init__(self, dir_path) -> None:
        self.__dir_path = dir_path

    def __str__(self) -> str:
        return f"<DumpReader: {self.__dir_path}>"

    @staticmethod
    def __create_file_name(row: dict) -> str:
        return f"{row[ID_COLUMN]}-{row[STATUS_CODE_COLUMN]}.gz"

    @staticmethod
    def __parse_file_name(file_name: str) -> dict[str, Any]:
        file_name_splited = file_name.split('\\')[-1].split('-')

        return { ID_COLUMN: int(file_name_splited[0]), STATUS_CODE_COLUMN : file_name_splited[1][:-3] }

    @staticmethod
    def write_item(dir_path: str, row: dict) -> None:
        if not os.path.isdir(dir_path): os.mkdir(dir_path)
        path = os.path.join(dir_path, DumpReader.__create_file_name(row))
        file = gzip.open(path, 'wb')
        file.write(row[CONTENT_COLUMN])
        file.close()

    @staticmethod
    def read_item(path: str) -> dict:
        file = gzip.open(path, 'rb')
        content = file.read()
        file.close()
        parsed_file_name = DumpReader.__parse_file_name(path)
        return { CONTENT_COLUMN: content, **parsed_file_name}

    @staticmethod
    def read(dir_path: str, count=-1, offset=0) -> 'list[dict]':
        """
        Прочитать дамп.

        :dir_path: Путь до директории дампа
        :count: Количество файлов для чтения. -1 - Прочитать все.
        :offset: Количество файлов для пропуска
        :return: Формат возвращаемых данных: 
        { 'id': 'id', 'Content': 'Content', 'StatusCode': 'StatusCode' }
        """ 
        files = os.listdir(dir_path)
        data = []
        index = offset
        files_count = 0

        for file_index, file in enumerate(files):
            if files_count == count: break 
            if file_index < index: continue
            file_path = os.path.join(dir_path, file)
            data.append(DumpReader.read_item(file_path))
            files_count += 1

        return data

    @staticmethod
    def write(dir_path: str, data: 'list[dict]') -> None:
        """
        Создание дампа.

        :dir_path: Путь до папки дампа
        :data: Данные для сжатия. Необходимый формат 
        { 'id': 'id', 'Content': 'Content', 'StatusCode': 'StatusCode' }
        """ 
        for index, row in enumerate(data):
            DumpReader.write_item(dir_path=dir_path, row=row)

    @staticmethod
    def get_last_index(dir_path: str) -> int:
        """
        Получение последнего индекса в папке дампа

        :dir_path: путь к директории дампа
        """ 
        if not os.path.isdir(dir_path): return -1
        files = os.listdir(dir_path)
        if len(files) == 0: return -1
        files_indexes: list[int] = list(map(lambda x: DumpReader.__parse_file_name(x)[ID_COLUMN], files))
        return max(files_indexes)

    @staticmethod
    def save_to_html(path: Any=None, row: Any=None, to_path: str = 'sample.html') -> None:
        """
        Сохранение дампа страницы в HTML

        :path: Путь до дампа страницы 
        :row: Страница
        (:path: или :row:)
        :to_path: Куда сохранить. По умолчанию 'sample.html'
        """ 
        row = row if row else DumpReader.read_item(path)
        content: bytes  = row['Content']
        file = open(to_path, "w", encoding="utf-8")
        file.write(content.decode("utf-8"))
        file.close()

if __name__ == '__main__':
    import requests

    r = requests.get('https://www.bbc.com/persian/science/2015/03/150320_l45_solar_eclipse')
    row = {ID_COLUMN : 1, CONTENT_COLUMN: r.content, STATUS_CODE_COLUMN: r.status_code}

    DumpReader.write_item(dir_path='test', row=row)
    print(DumpReader.read_item(path='test\\1-200.gz'))