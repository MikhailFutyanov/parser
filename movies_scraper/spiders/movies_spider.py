import scrapy

class MoviesSpider(scrapy.Spider):
    name = "movies_spider"
    allowed_domains = ["ru.wikipedia.org"]
    start_urls = ["https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту"]
    custom_settings = {
        'FEED_EXPORT_ENCODING': 'utf-8',
        'DEPTH_LIMIT': 0,   
        'CONCURRENT_REQUESTS': 16,
        'RETRY_TIMES': 10,
        'DOWNLOAD_DELAY': 0.1,   # Задержка в 0.1 секунду между запросами
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' \
                      'AppleWebKit/537.36 (KHTML, like Gecko) ' \
                      'Chrome/98.0.4758.102 Safari/537.36',
        'ROBOTSTXT_OBEY': False,  
    }

    def parse(self, response):
        self.logger.info(f"Начало парсинга страницы")
        # Извлекаем ссылки на фильмы
        film_links = response.xpath('//div[contains(@class, "mw-category")]//ul/li/a/@href').getall()
        self.logger.info(f"Найдено ссылок: {film_links}")
        

        for link in film_links:
            url = response.urljoin(link)
            self.logger.info(f"Найденная ссылка на фильм: {url}")
            yield scrapy.Request(url, callback=self.parse_movie, dont_filter=True)

        # Находим ссылку на следующую страницу
        next_page = response.xpath('//a[contains(text(), "Следующая страница")]/@href').get()
        if next_page:
            next_page_url = response.urljoin(next_page)
            self.logger.info(f"Переход на следующую страницу: {next_page_url}")
            yield scrapy.Request(next_page_url, callback=self.parse)
        else:
            self.logger.info("Больше страниц не найдено.")

    def parse_movie(self, response):
        # Пропускаем категории
        if "Категория:" in response.url:
            self.logger.warning(f"Пропускаю категорию: {response.url}")
            return

        movie = {}
        movie["Название"] = response.xpath('//*[@id="firstHeading"]/span/text()').get(default="Неизвестное название")
        self.logger.info(f"Спарсено название фильма: {movie['Название']}")

        # Парсим данные из инфобокса
        infobox = response.xpath('//table[contains(@class, "infobox")]')
        if not infobox:
            self.logger.warning(f"Инфобокс не найден на странице: {response.url}")
            return

        rows = infobox[0].xpath('.//tr')
        for row in rows:
            key = row.xpath('.//th//text()').get()
            if not key:
                continue
            key = key.strip()
            value = " ".join(row.xpath('.//td//text()').getall()).strip()

            if key in ["Жанр", "Жанры"]:
                movie["Жанр"] = [item.strip() for item in value.split(";")]
            elif key in ["Режиссер", "Режиссёр", "Режиссёры"]:
                movie["Режиссер"] = [item.strip() for item in value.split(";")]
            elif key in ["Страна", "Страны"]:
                movie["Страна"] = [item.strip() for item in value.split(";")]
            elif key in ["Год", "Год выпуска", "Премьера"]:
                movie["Год"] = value

        yield movie
