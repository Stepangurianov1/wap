from bs4 import BeautifulSoup
import requests
import time
import csv
from selenium import webdriver
from multiprocessing import Pool
import psycopg2

url = 'https://dzen.ru/?yredirect=true'


class Main():
    def __init__(self, url):
        self.url = url
        self.count = 0
        self.time = 0
        self.file_name = ''
        self.time_sleep = 5  # задержка, чтобы пользователь успел ввести капчу, если она необходима
        self.csv_file = 'data.csv'
        self.path_to_driver = 'C:/Users/User/Documents/тесты работа/Карта сайта/chromedriver.exe'  # не заубдьте
        # заменить при запуске программы
        self.driver = webdriver.Chrome(executable_path=self.path_to_driver)

    def get_link(self):  # функция получения ссылнок
        list_links = []
        start = time.time()
        request = requests.get(self.url)
        html = BeautifulSoup(request.content, 'html.parser')
        body = html.find('body')
        if bool(body):
            """Данный фаргмент кода нужен так как многие сайты трубуют от пользовалтеля ввести капчу, а также body 
            загружается после выполнения запроса к сайту через requests. Очевидно, что сильно тормозит программу,
            но без этого никак так как капчу ввести надо"""
            self.driver.get(url=self.url)
            time.sleep(self.time_sleep)
            content = self.driver.page_source
            html = BeautifulSoup(content, 'html.parser')
            body = html.find('body')
        try:
            links = body.find_all('a')
        except AttributeError:
            print("Ошибка! Скорее всего не успели ввести капчу")
            return
        if self.url[:5] == 'https':
            self.file_name = (self.url[8:])
        else:
            self.file_name = (self.url[7:])
        with open(f'{self.file_name}.csv', mode='w', encoding='utf-8') as file:
            writer_file = csv.writer(file)
            writer_file.writerow(['Тело ссылки', 'Ссылка'])
            for link in links:
                string = link.string
                href = link.get('href')
                if string and href:
                    self.count += 1
                    writer_file.writerow([string, href])

                list_links.append((string, href))
        self.time = time.time() - start - self.time_sleep
        self.driver.close()
        self.driver.quit()
        return iter(list_links)

    def write_csv(self):  # Функиция записи в csv файл в соответствии с ТЗ

        with open(self.csv_file, mode='a', encoding='utf-8') as file:
            self.get_link()
            writer = csv.writer(file)
            writer.writerow([self.url, self.time, self.count, self.file_name])

    @classmethod
    def run_write_csv(cls, arg):  # Функция создает экзмепляр класса и вызывает метод для записи в csv, необходима
        # для параллельного вызова
        obj = cls(arg)
        obj.write_csv()

    def write_from_db(self):  # Функция для записи в БД (В коде не вызывается, но все раюотает )
        data = self.get_link()
        DBNAME = 'work_db'
        USER = 'user_work'
        PASSWORD = '2000'
        HOST = 'localhost'
        conn = psycopg2.connect(dbname=DBNAME, user=USER,
                                password=PASSWORD, host=HOST)
        cursor = conn.cursor()
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS tusks(name_link text not null, link text)')
        args = ','.join(cursor.mogrify("(%s,%s)", i).decode('utf-8')
                        for i in data)
        cursor.execute("INSERT INTO tusks VALUES " + args)
        conn.commit()
        conn.close()


if __name__ == '__main__':
    web = iter(['http://crawler-test.com', 'http://google.com', 'https://vk.com', 'https://dzen.ru', 'https'
                                                                                               '://stackoverflow.com'])
    with open('data.csv', mode='w', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['URL сайта', 'Время обработки', 'Количество наденных ссылок', 'Имя файла с результатом'])
    with Pool(8) as p:
        p.map(Main.run_write_csv, web)
