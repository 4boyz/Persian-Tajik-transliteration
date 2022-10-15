import gzip
import os.path
import os

ID_COLUMN = 'id'
CONTENT_COLUMN = 'Content'
STATUS_CODE_COLUMN = 'StatusCode'

class DumpReader:
    @staticmethod
    def __create_file_name(row: dict) -> str:
        return f"{row[ID_COLUMN]}-{row[STATUS_CODE_COLUMN]}.gz"

    @staticmethod
    def __parse_file_name(file_name: str) -> dict:
        file_name_splited = file_name.split('-')
        return { ID_COLUMN: int(file_name_splited[0]), STATUS_CODE_COLUMN : file_name_splited[1] }

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
    def read(dir_path: str, read_count=-1, read_span=0) -> list[dict]:
        files = os.listdir(dir_path)
        data = []
        index = read_span
        files_count = 0

        for file_index, file in enumerate(files):
            if files_count == read_count: break 
            if file_index < index: continue
            data.append(DumpReader.read_item(file))

        return data

    @staticmethod
    def write(dir_path: str, data: list[dict]) -> None:
        for index, row in enumerate(data):
            DumpReader.write_item(dir_path=dir_path, row=row)

    @staticmethod
    def get_last_index(dir_path: str) -> int:
        if not os.path.isdir(dir_path): return -1
        files = os.listdir(dir_path)
        if len(files) == 0: return -1
        return DumpReader.__parse_file_name(files[-1])[ID_COLUMN]

if __name__ == '__main__':
    import requests

    r = requests.get('https://www.bbc.com/persian/science/2015/03/150320_l45_solar_eclipse')
    row = {ID_COLUMN : 1, CONTENT_COLUMN: r.content, STATUS_CODE_COLUMN: r.status_code}

    DumpReader.write_item(dir_path='test', row=row)
    print(DumpReader.read_item(path='test\\1-200.gz'))