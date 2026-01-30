import asyncio
import aiohttp
import asyncpg
import json
import logging
import os
from datetime import datetime
from fake_useragent import UserAgent
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# Конфигурация
DB_CONFIG = {
    "dsn": f"postgres://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}",
    "min_size": 5,
    "max_size": 10
    }

BASE_URL = "https://bo.nalog.gov.ru"
MAX_SEARCH_WORKERS = 3       # Не ставь много, забанят на поиске
MAX_DETAIL_WORKERS = 2      # Деталей можно собирать больше
headers_template = {
    'Accept': '*/*',
    'Accept-Language': 'ru,en;q=0.9,de;q=0.8',
    'Connection': 'keep-alive',
    'Cookie': '_ym_uid=1691573085528894859; _ym_d=1691573085; _ym_isad=2; disclaimed=true',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin'
}

class Scraper:

    def __init__(self, db_pool):
        self.pool = db_pool
        self.ua = UserAgent()
        # Очередь для задач поиска (параметры поиска)
        self.search_queue = asyncio.Queue()
        # Очередь для задач сбора деталей (ID организаций)
        self.detail_queue = asyncio.Queue()
        # Сет для дедупликации ID в памяти, чтобы не добавлять в очередь дубли
        self.seen_ids = set()


    def get_headers(self):
        h = headers_template.copy()
        h["User-Agent"] = self.ua.random
        return h
    

    async def fetch_json(self, session, url, params=None):
        """Обертка над запросом с повторами"""
        for _ in range(3):
            try:
                async with session.get(url, params=params, headers=self.get_headers(), timeout=15) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    else:
                        logger.warning(f"Status {resp.status} for {url}. Retrying...")
                        await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Network error {url}: {e}")
                await asyncio.sleep(2)
        return {} # Возвращаем пустой dict после всех неудач
    

    async def check_if_search_done(self, okved, region, year, page):
        """Проверка для возобновления работы (Resumability)"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT 1 FROM raw.raw_dump_data_search_params 
                   WHERE okved=$1 AND region_name=$2 AND year=$3 AND total_page=$4 LIMIT 1""", 
                okved, region, year, page
            )
            return row is not None
        

    async def save_search_dump(self,  okved, region, page, year, payload):
        """Сохранение сырого JSON поиска (ELT)"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO raw.raw_dump_data (okved, year, page, region_name, payload) 
                   VALUES ($1, $2, $3, $4, $5)""",
                okved, year, page, region, json.dumps(payload)
            )

    async def save_search_params_log(self, okved, year, region, total_pages, total_elements):
        """Фиксация факта, что мы отработали этот поиск"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO raw.raw_dump_data_search_params 
                   (okved, year, region_name, total_page, total_elements) 
                   VALUES ($1, $2, $3, $4, $5) 
                   ON CONFLICT DO NOTHING""",
                okved, year, region, total_pages, total_elements
            )


    async def save_details_dump(self, org_id, info_payload, bfo_payload):
        """Сохранение деталей. Триггеры базы сами разберут JSONB"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Сохраняем инфо (триггер split_org_info сработает тут)
                await conn.execute(
                    """INSERT INTO raw.raw_organization_dump_data (id, payload) 
                       VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET last_date = NOW(), payload = EXCLUDED.payload""",
                    org_id, json.dumps(info_payload)
                )
                # Сохраняем БФО (это пока просто дамп, триггеров на него у тебя вроде нет или простой)
                await conn.execute(
                    """INSERT INTO raw.raw_organization_bfo_dump_data (id, payload) 
                       VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET last_date = NOW(), payload = EXCLUDED.payload""",
                    org_id, json.dumps(bfo_payload)
                )

# --- WORKER: Поиск ---
    async def search_worker(self, session):
        # Получаем регионы один раз при старте воркера
        async with self.pool.acquire() as conn:
            # fetch возвращает список записей, берем их сразу
            rows = await conn.fetch("""SELECT "name" FROM raw.russian_federal_subjects""")
            list_regions = [row['name'] for row in rows]

        
        # Получаем список лет
        context_data = await self.fetch_json(session, f"{BASE_URL}/nbo/context")
        # Защита, если periods нет в ответе
        list_years = context_data.get('bfoPeriods', []) if context_data else []
        
        while True:
            # В очереде только ОКВЭД, так как это стартовая точка
            okved = await self.search_queue.get()
            
            try:
                logger.info(f"🔍 Start processing OKVED: {okved}")
                
                # УРОВЕНЬ 1: Проверяем чистый ОКВЭД
                first_page = await self.fetch_json(session, f"{BASE_URL}/advanced-search/organizations", 
                                                    {"okved": okved, "page": 0, "size": 100})
                
                total_pages = first_page.get("totalPages", 0)
                total_elements = first_page.get("totalElements", 0)
                
                # Сценарий А: Мало записей, качаем всё сразу
                if total_pages <= 100:
                    await self.process_all_pages(session, okved, None, None, first_page, total_pages)
                    # Логируем успех для ОКВЭДА целиком
                    await self.save_search_params_log(okved, None, None, total_pages, total_elements)
                
                # Сценарий Б: Много записей, идем по РЕГИОНАМ
                else:
                    logger.info(f"⚠️ OKVED {okved} has {total_elements} elements. Drilling down to Regions.")
                    
                    for region_name in list_regions:
                        # УРОВЕНЬ 2: ОКВЭД + Регион
                        reg_page = await self.fetch_json(session, f"{BASE_URL}/advanced-search/organizations", 
                                                        {"okved": okved, "address": region_name, "page": 0, "size": 100})
                        
                        reg_total_pages = reg_page.get("totalPages", 0)
                        reg_total_elements = reg_page.get("totalElements", 0)

                        if reg_total_pages == 0:
                            continue

                        # Если в регионе мало записей - качаем
                        if reg_total_pages <= 100:
                            await self.process_all_pages(session, okved, region_name, None, reg_page, reg_total_pages)
                            # Логируем успех для ПАРЫ (ОКВЭД + РЕГИОН)
                            await self.save_search_params_log(okved, None, region_name, reg_total_pages, reg_total_elements)
                        
                        # Сценарий В: В регионе дофига записей (Москва, например), идем по ГОДАМ
                        else:
                            for year in list_years:
                                # УРОВЕНЬ 3: ОКВЭД + Регион + Год
                                year_page = await self.fetch_json(session, f"{BASE_URL}/advanced-search/organizations", 
                                                                    {"okved": okved, "address": region_name, "period": year, "page": 0, "size": 100})
                                
                                year_total_pages = year_page.get("totalPages", 0)
                                year_total_elements = year_page.get("totalElements", 0)
                                
                                if year_total_pages > 0:
                                    # Тут уже качаем сколько есть, даже если больше 100 (но скорее всего API отдаст только 100)
                                    # Либо можно поставить limit=100
                                    await self.process_all_pages(session, okved, region_name, year, year_page, year_total_pages)
                                    
                                    # Логируем успех для ТРОЙКИ (ОКВЭД + РЕГИОН + ГОД)
                                    await self.save_search_params_log(okved, year, region_name, year_total_pages, year_total_elements)
                                
                                await asyncio.sleep(0.2) # Микро-пауза
            
            except Exception as e:
                logger.error(f"Error in search worker ({okved}): {e}", exc_info=True)
            finally:
                self.search_queue.task_done()

    # Вспомогательный метод, чтобы не копипастить циклы
    async def process_all_pages(self, session, okved, region, year, first_page_data, total_pages):
        # Обрабатываем 0-ю страницу
        await self.process_search_page(first_page_data, okved, region=region, year=year, page=0)
        
        # API часто ограничивает выдачу 100 страницами (1000 записей)
        limit = min(total_pages, 100)
        
        for page in range(1, limit):
            params = {"okved": okved, "page": page, "size": 100}
            if region: params["address"] = region
            if year: params["period"] = year
            
            data = await self.fetch_json(session, f"{BASE_URL}/advanced-search/organizations", params)
            await self.process_search_page(data, okved, region=region, year=year, page=page)
            await asyncio.sleep(0.5)

    async def process_search_page(self, data, okved, region=None, year=None, page=0):
        if not data or 'content' not in data:
            return
            
        # ELT: Сохраняем сырой поиск
        await self.save_search_dump(okved, region, page, year, data)

        # Отправляем ID в очередь на детальный парсинг
        for item in data['content']:
            org_id = item.get('id')
            if org_id and org_id not in self.seen_ids:
                self.seen_ids.add(org_id)

                try:
                    self.detail_queue.put_nowait(org_id)
                except asyncio.QueueFull:
                    logger.warning(f"Detail queue full, waiting for space...")
                    await self.detail_queue.put(org_id)



    # --- WORKER: Детали ---
    async def detail_worker(self, session):
        while True:
            org_id = await self.detail_queue.get()
            try:
                # !!! ТУТ ПАРАЛЛЕЛИЗМ !!!
                # Запускаем оба запроса одновременно
                url_info = f"{BASE_URL}/nbo/organizations/{org_id}"
                url_bfo = f"{BASE_URL}/nbo/organizations/{org_id}/bfo/"

                # asyncio.gather ждет завершения обоих запросов
                # return_exceptions=True, чтобы ошибка в одном не крашила другой (хотя тут нам нужны оба)
                res_info, res_bfo = await asyncio.gather(
                    self.fetch_json(session, url_info),
                    self.fetch_json(session, url_bfo)
                )

                if isinstance(res_info, Exception):
                    logger.error(f"Error fetching info for {org_id}: {res_info}")
                    res_info = {}
                    
                if isinstance(res_bfo, Exception):
                    logger.error(f"Error fetching BFO for {org_id}: {res_bfo}")
                    res_bfo = {}

                if res_info:
                    await self.save_details_dump(org_id, res_info, res_bfo)
                else:
                    logger.warning(f"No info data for {org_id}, skipping save")
            except Exception as e:
                logger.error(f"Error detail worker {org_id}: {e}")
            finally:
                self.detail_queue.task_done()

    async def producer(self):
        """Генерирует задачи для поиска"""
        for i in range(1, 100): 
            for j in range(1, 100):
                okved = f"{i:02d}.{j:02d}"
                # ИСПРАВЛЕНО: кладем просто строку, без лишних скобок
                await self.search_queue.put(okved)

async def main():
    pool = None
    try:
        # Создаем пул БД ПЕРЕД сессией
        pool = await asyncpg.create_pool(
            DB_CONFIG["dsn"], 
            min_size=DB_CONFIG["min_size"], 
            max_size=DB_CONFIG["max_size"]
        )
        
        async with aiohttp.ClientSession() as session:
            scraper = Scraper(pool)

            # Запускаем воркеры
            search_tasks = [
                asyncio.create_task(scraper.search_worker(session)) 
                for _ in range(MAX_SEARCH_WORKERS)
            ]
            detail_tasks = [
                asyncio.create_task(scraper.detail_worker(session)) 
                for _ in range(MAX_DETAIL_WORKERS)
            ]

            # Запускаем генератор задач
            await scraper.producer()

            # Ждем, пока очередь поиска опустеет
            logger.info("Waiting for search queue to finish...")
            await scraper.search_queue.join()
            
            # Ждем, пока очередь деталей опустеет
            logger.info("Waiting for detail queue to finish...")
            await scraper.detail_queue.join()

            # Отменяем воркеры (они в бесконечном цикле)
            for task in search_tasks + detail_tasks:
                task.cancel()
            
            # Ждем завершения всех задач
            try:
                await asyncio.gather(*search_tasks, *detail_tasks, return_exceptions=True)
            except Exception as e:
                logger.error(f"Error during task cancellation: {e}")
    
    finally:
        # Закрываем пул в finally блоке
        if pool:
            await pool.close()
            logger.info("Database pool closed.")
        logger.info("Done.")

# Закрываем пул только после завершения всех задач
        await pool.close()
        logger.info("Done.")

if __name__ == "__main__":
    # Для Windows может понадобиться:
    # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())