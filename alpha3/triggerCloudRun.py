import asyncio
import json

import aiohttp


async def post(session, league, link, season):
    async with session.post("https://fixture-link-scraper-jrn5p5gjaa-uc.a.run.app", headers={'Content-Type': 'application/json'},
              data=json.dumps({"league": league,
                               "link": link,
                               "season": season})) as response:
        json_response = await response.read()
        if (response.status != 200):
            raise Exception("HTTP POST to Cloud Run responded with status code {}".format(response.status))

        return json_response


async def runner(urls):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
        gathered_responses = await asyncio.gather(*[post(session, league, link, season) for league, season, link in urls])
    print(gathered_responses)
    return gathered_responses
