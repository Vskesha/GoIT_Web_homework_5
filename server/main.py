import asyncio
import logging
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict

import aiohttp
import websockets
import names
from aiopath import AsyncPath
from aiofile import async_open
from websockets import WebSocketServerProtocol
from websockets.exceptions import ConnectionClosedOK
from jinja2 import Environment, FileSystemLoader


logging.basicConfig(level=logging.INFO)
BASE_URL = "https://api.privatbank.ua/p24api/exchange_rates?date="  # + '01.12.2014'
CACHE_FILE = "cache.json"
ALL_CURRENCIES = {
    "SEK",
    "DKK",
    "GEL",
    "CZK",
    "TRY",
    "KZT",
    "ILS",
    "AZN",
    "JPY",
    "CAD",
    "BYN",
    "UAH",
    "NOK",
    "USD",
    "CHF",
    "CNY",
    "MDL",
    "HUF",
    "XAU",
    "EUR",
    "SGD",
    "PLN",
    "UZS",
    "TMT",
    "AUD",
    "GBP",
}


class HttpError(Exception):
    pass


class Table:
    environment = Environment(loader=FileSystemLoader("."))
    template = environment.get_template("table.jinja")

    async def make_table(self, headers: List[str], data: List[Dict]):
        """
        Makes html table from given exchange data
        """
        subheads = ["buy", "sell", "NBU"]
        table = self.template.render(
            headers=headers, data=data, subheads=subheads, days=len(data)
        )
        return table


class DataFetcher:
    def __init__(self, base_url: str, cache_file: str):
        self.base_url = base_url
        self.cache_file = cache_file
        self.currencies = ["USD", "EUR"]

    async def add_currency(self, currency: str):
        if currency in ALL_CURRENCIES:
            if currency not in self.currencies:
                self.currencies.append(currency)
                return f"{currency} was added. Current currencies: {', '.join(self.currencies)}"
            return f"{currency} is already in the list. Current currencies: {', '.join(self.currencies)}"
        return f"There is no such currency. Current currencies: {', '.join(self.currencies)}"

    async def remove_currency(self, currency: str):
        if currency in self.currencies:
            self.currencies.remove(currency)
            return f"{currency} was removed. Current currencies: {', '.join(self.currencies)}"
        return f"{currency} is not in the list. Current currencies: {', '.join(self.currencies)}"

    async def request(self, url: str):
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result
                    else:
                        raise HttpError(f"Error status: {response.status} for {url}")
            except (aiohttp.ClientConnectorError, aiohttp.InvalidURL) as err:
                raise HttpError(f"Connection error: {url}", str(err))

    async def load_from_cache(self):
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as pool:
            result = await loop.run_in_executor(pool, self.load_json_sync)
            return result

    def load_json_sync(self):
        """loads data from cache file"""
        try:
            with open(self.cache_file, "r") as fd:
                return json.load(fd)
        except FileNotFoundError:
            return {}

    async def save_to_cache(self, data: Dict):
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as pool:
            await loop.run_in_executor(pool, self.dump_json_sync, data)

    def dump_json_sync(self, data: Dict):
        """saves data to cache file"""
        with open(self.cache_file, "w") as fd:
            json.dump(data, fd, ensure_ascii=False, indent=4)

    async def get_exchanges(self, days: int):
        cache = await self.load_from_cache()
        today = datetime.now()
        result = []
        for i in range(days):
            date_ = today - timedelta(days=i)
            date_ = date_.strftime("%d.%m.%Y")
            if date_ in cache:
                date_data = cache[date_]
            else:
                logging.info(f"Fetching data from {self.base_url}{date_}")
                date_data = await self.request(f"{self.base_url}{date_}")
                logging.info(f"Fetched data for {date_}")
                cache[date_] = date_data
            rates = {
                currency: {"buy": "---", "sell": "---", "NBU": "---"}
                for currency in self.currencies
            }
            logging.info(f"Currencies: {self.currencies}")
            for cur_dict in date_data["exchangeRate"]:
                currency = cur_dict.get("currency")
                if currency in self.currencies:
                    rates[currency] = {
                        "buy": f'{cur_dict.get("purchaseRate"):0.2f}'
                        if cur_dict.get("purchaseRate")
                        else "---",
                        "sell": f'{cur_dict.get("saleRate"):0.2f}'
                        if cur_dict.get("saleRate")
                        else "---",
                        "NBU": f'{cur_dict.get("saleRateNB"):0.2f}'
                        if cur_dict.get("saleRateNB")
                        else "---",
                    }

            result.append({date_: rates})

        await self.save_to_cache(cache)

        return self.currencies, result


class Server:
    def __init__(self):
        self.clients = set()
        self.fetcher = DataFetcher(BASE_URL, CACHE_FILE)
        self.table = Table()
        self.log_exchange_file = AsyncPath('log_exchange.log')

    async def register(self, ws: WebSocketServerProtocol):
        ws.name = names.get_full_name()
        self.clients.add(ws)
        logging.info(f"{ws.remote_address} connects")

    async def unregister(self, ws: WebSocketServerProtocol):
        self.clients.remove(ws)
        logging.info(f"{ws.remote_address} disconnects")

    async def send_to_clients(self, message: str):
        if self.clients:
            [await client.send(message) for client in self.clients]

    async def ws_handler(self, ws: WebSocketServerProtocol):
        await self.register(ws)
        try:
            await ws.send(
                "<h2>Welcome to chat! </h2>"
                "Use <b>exchange add <i>currency_lit</i></b> to add or"
                "<b>exchange remove <i>currency_lit</i></b> to remove currency (USD or EUR or another).<br>"
                "Type <b>exchange <i>number</i></b> for showing exchange rates "
                "of <i><b>number</b></i> previous days (1 by default, max 10)"
                "<h3>or simly send messages to all in chat</h3>"
            )
            await self.handle_messages(ws)
        except ConnectionClosedOK:
            pass
        finally:
            await self.unregister(ws)

    async def handle_messages(self, ws: WebSocketServerProtocol):
        async for message in ws:
            if message.startswith("exchange"):
                try:
                    args = message.split()
                    if len(args) > 2 and args[1].lower() == "add":
                        msg = await self.fetcher.add_currency(args[2].upper())
                        await ws.send(msg)
                    elif len(args) > 2 and args[1].lower() == "remove":
                        msg = await self.fetcher.remove_currency(args[2].upper())
                        await ws.send(msg)
                    else:
                        days = 1
                        if len(args) > 1 and args[1].isdigit():
                            days = int(args[1])
                            days = min(10, max(1, days))
                        headers, exchanges = await self.fetcher.get_exchanges(days)
                        html_table = await self.table.make_table(headers, exchanges)
                        await ws.send(html_table)
                except HttpError as err:
                    logging.error(err)
                    await ws.send("Something went wrong")
                async with async_open(self.log_exchange_file, "a") as afp:
                    await afp.write(f"{datetime.now()} {ws.remote_address}: {ws.name}: {message}\n")
            else:
                await self.send_to_clients(f"{ws.name}: {message}")


async def main():
    server = Server()
    async with websockets.serve(server.ws_handler, "localhost", 8080):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
