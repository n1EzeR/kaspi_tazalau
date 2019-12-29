import asyncio

from code import processing
from code import parser


async def process_data():
    await parser.compile_data()
    processing.process_data()


if __name__ == '__main__':
    asyncio.run(process_data())
