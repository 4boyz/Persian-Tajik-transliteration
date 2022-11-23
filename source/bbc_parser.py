from bs4 import BeautifulSoup

class BBCParser:

    @staticmethod
    def __parse_text(content: bytes, select_query: str) -> list[str]:
        soup = BeautifulSoup(content, features='html.parser')
        text = []
        text_nodes = soup.select(select_query)
        for node in text_nodes:
            if len(node.text) > 0:
                text.append(node.text)
        return text
    
    @staticmethod
    def parse_text_tj(content: bytes) -> list[str]:
        parsed_text = BBCParser.__parse_text(content=content, select_query='[property=articleBody] > p')
        return parsed_text

    @staticmethod
    def parse_text_pers(content: bytes) -> list[str]:
        parsed_text = BBCParser.__parse_text(content=content, select_query='main > div > p')
        if len(parsed_text) == 0: parsed_text = BBCParser.__parse_text(content=content, select_query='.bodytext > p')
        return parsed_text