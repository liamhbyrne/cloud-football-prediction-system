import asyncio
import json

import aiohttp

"""
The following 2 asynchronous functions manage sending several HTTP Post requests to a Google Cloud Run end point at once
"""

async def post(session, league, link, season):
    """
    This function is called by the runner() to send a HTTP POST
    """
    async with session.post("https://soccerway-fetcher-jrn5p5gjaa-uc.a.run.app/",
                  headers={'Content-Type': 'application/json'},
                  data=json.dumps({"league": league,
                                   "link": link,
                                   "season": season})) as response:
        json_response = await response.read()
        if (response.status != 200):
            raise Exception("HTTP POST to Cloud Run responded with status code {}".format(response.status))

        return json_response


async def runner(urls):
    """
    This function uses an asynchronous IO session to enable hundreds of HTTP POST requests to be sent ASAP
    """
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
        gathered_responses = await asyncio.gather(*[post(session, league, link, season) for league, season, link in urls])
    return gathered_responses
