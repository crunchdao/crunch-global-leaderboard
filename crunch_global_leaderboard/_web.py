import os
from multiprocessing.pool import ThreadPool
from textwrap import dedent
from typing import Dict, Optional, Tuple
from warnings import catch_warnings, filterwarnings

import bs4
import requests
from openai import OpenAI
from tqdm.auto import tqdm
from urllib3.exceptions import InsecureRequestWarning

from crunch_global_leaderboard._model import InstitutionName, University

CHROME_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"


OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
openai_client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),
)


def _get_html(
    url: str,
) -> Optional[str]:
    try:
        with catch_warnings():
            filterwarnings("ignore", category=InsecureRequestWarning)

            response = requests.get(
                url,
                headers={
                    "User-Agent": CHROME_USER_AGENT,
                },
                verify=False,
                timeout=(10, 5)
            )

        response.raise_for_status()
    except requests.RequestException as error:
        print(f"{url}: {error}")
        return None

    response.encoding = "utf-8"
    return response.text


def get_site_description(
    url: str,
    *,
    quiet: bool = False,
) -> Optional[str]:
    html = _get_html(url)
    if html is None:
        return None

    soup = bs4.BeautifulSoup(html, "lxml")

    properties: Dict[str, Optional[str]] = {}
    for meta in soup.find_all("meta"):
        attrs: Dict[str, str] = meta.attrs  # type: ignore

        key = attrs.get("property") or attrs.get("name")
        if key is None:
            continue

        content = attrs.get("content")
        properties[key] = content

    description = (
        properties.get("description")
        or properties.get("og:description")
        or properties.get("twitter:description")
    )

    if not quiet:
        print(f"{url}: original: {description}")

    return description


def get_site_descriptions(
    universities: Dict[InstitutionName, University],
    rephrase: bool = False,
    quiet: bool = False,
    max_workers: int = 16,
):
    with ThreadPool(processes=max_workers) as pool:
        def fetch(entry: Tuple[InstitutionName, University]):
            key, university = entry

            url = university["url"]
            if url is None:
                return (key, None)

            description = get_site_description(
                url,
                quiet=quiet,
            )

            if description is not None and rephrase:
                description = rephrase_description(
                    university,
                    description,
                )

                if not quiet:
                    print(f"{url}: rephrased: {description}")

            return (key, description)

        return {
            key: description
            for key, description in tqdm(pool.imap_unordered(fetch, universities.items()), total=len(universities))
        }


def rephrase_description(
    university: University,
    description: str,
):
    fallback_sentence = "Not possible"

    system_prompt = dedent(f"""
        You are given a university name, country and website url. You are also given the website opengraph description.

        From all of those information you must write a small "about me" section that would be displayed on a website.

        If you do not have enough information, the description is contains something else than what the school is (like a website navigation), or you just don't enough about the school, just say "{fallback_sentence}".

        If the description is not in english, you must translated it in english.

        Your message must just be a very simple description of the school, not a description.

        Your message must not contains the university name or abbreviation: "ABC is a COUNTRY university..." should be "A COUNTRY university...".
    """)

    user_prompt = dedent(f"""
        Name: {university["name"]}
        URL: {university["url"]}
        Country: {university["country_alpha3"]}
        Website Description: {description}
    """)

    response = openai_client.responses.create(
        model="gpt-4.1-nano",
        instructions=system_prompt,
        input=user_prompt,
        temperature=0.0,
    )

    output_text = response.output_text
    if output_text.lower() == fallback_sentence.lower():
        return None

    return output_text
