import pandas as pd
import requests

from dump_reader import DumpReader, ID_COLUMN, CONTENT_COLUMN, STATUS_CODE_COLUMN

URL_COL_NAME = 'BeautyLink'

class Dumper:
    @staticmethod
    def __get_content(url: str) -> dict:
        result = requests.Response()
        content = ''
        status_code = ''
        if url:
            try:
                result = requests.get(url)
                content = result.content
                status_code = result.status_code
            except Exception as e:
                print(f"Error: {str(e)}")
        print(f"{url} - {status_code}")
        return {
            CONTENT_COLUMN : content,
            STATUS_CODE_COLUMN :  status_code,
        }

    @staticmethod
    def write_dump(save_dir: str, data_path : str=None, data_frame: pd.DataFrame=None, url_col_name: str=URL_COL_NAME, id_col_name: str=ID_COLUMN) -> None:
        if not data_path and not data_frame: raise Exception('Не указан источник данных')
        data = pd.read_csv(data_path, on_bad_lines='skip') if data_path else data_frame
        last_index = DumpReader.get_last_index(save_dir)

        for index, row in data.iterrows():
            row_index = int(row[id_col_name])
            if row_index < last_index: continue
            DumpReader.write_item(dir_path=save_dir, row={
                ID_COLUMN : row_index,
                **Dumper.__get_content(row[url_col_name])
            })


if __name__ == '__main__':
    
    # Dumper.write_dump('data\\beauty_links_persian_cleared.csv', 'data\\persian_dump')
    Dumper.write_dump('data\\tajik_dump', 'data\\beauty_links_tajik_cleared.csv')