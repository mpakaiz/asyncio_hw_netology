import asyncio
import datetime
from typing import List


import aiohttp
from more_itertools import chunked

from models import Session, SwapiPeople, init_db

CHUNK_SIZE = 10

async def fetch_and_join_data(session, urls, key):
    tasks = [fetch_data(session, url, key) for url in urls]
    results = await asyncio.gather(*tasks)
    return ', '.join(results)

async def fetch_data(session, url, key):
    response = await session.get(url)
    data = await response.json()
    return data.get(key, '')

async def get_person(person_id, session):
    response = await session.get(f"https://swapi.py4e.com/api/people/{person_id}/")
    person_data = await response.json()

    tasks = []

    if 'homeworld' in person_data:
        tasks.append(fetch_data(session, person_data['homeworld'], 'name'))

    for i in ['films', 'species', 'starships', 'vehicles']:
        if i in person_data:
            tasks.append(fetch_and_join_data(session, person_data[i], 'title' if i == 'films' else 'name'))
    results = await asyncio.gather(*tasks)

    for key, result in zip(['homeworld', 'films', 'species', 'starships', 'vehicles'], results):
        if key in person_data:
            person_data[key] = result

    return person_data

async def insert_to_db(people_dict: List[dict]):
    async with Session() as session:
        people = [SwapiPeople(
            birth_year=person.get('birth_year', ''),
            eye_color=person.get('eye_color', ''),
            films=person.get('films', ''),
            gender=person.get('gender', ''),
            hair_color=person.get('hair_color', ''),
            height=person.get('height', ''),
            homeworld=person.get('homeworld', ''),
            mass=person.get('mass', ''),
            name=person.get('name', ''),
            skin_color=person.get('skin_color', ''),
            species=person.get('species', ''),
            starships=person.get('starships', ''),
            vehicles=person.get('vehicles', ''),
        ) for person in people_dict]
        session.add_all(people)
        await session.commit()

async def main():
    await init_db()
    session = aiohttp.ClientSession()

    for people_id_chunk in chunked(range(1, 100), CHUNK_SIZE):
        coros = [get_person(person_id, session) for person_id in people_id_chunk]
        result = await asyncio.gather(*coros)
        print(result)
        asyncio.create_task(insert_to_db(result))

    await session.close()
    set_of_tasks = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*set_of_tasks)


if __name__ == "__main__":
    start = datetime.datetime.now()
    asyncio.get_event_loop().run_until_complete(main())
    print(datetime.datetime.now() - start)
