import asyncio
import aiohttp
from bs4 import BeautifulSoup
import urllib.parse
import csv
import PyPDF2
from io import BytesIO
from typing import List, Tuple, Optional
import os
from dotenv import load_dotenv
from openai import AsyncOpenAI
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='scraper.log'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize OpenAI API client
client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Number of articles to process
NUM_ARTICLES = 15  # You can change this value to process more articles

async def get_google_search_results(query: str, num_results: int = 10) -> List[str]:
    logger.info(f"Searching Google for query: {query}")
    query_encoded = urllib.parse.quote_plus(query)
    url = f"https://www.google.com/search?q={query_encoded}&num={num_results}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as response:
            if response.status != 200:
                logger.error(f"Failed to retrieve the web page. Status code: {response.status}")
                return []

            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            results = [urllib.parse.unquote(item['href'][7:item['href'].index('&')])
                       for item in soup.find_all('a', href=True)
                       if item['href'].startswith('/url?q=http')]
            logger.info(f"Found {len(results)} search results")
            return results[:NUM_ARTICLES]


async def extract_relevant_paragraph(text: str, query: str) -> str:
    logger.info("Extracting relevant paragraph with OpenAI")
    try:
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an AI that extracts relevant economic data from text."},
                {"role": "user",
                 "content": f"Extract a paragraph containing relevant data matching this query: '{query}'. The paragraph should contain related to Cannabis echonomics. If no relevant information is found, respond with 'No relevant information found.'\n\nText: {text[:4000]}"}
                # Limiting to 4000 characters to avoid token limits
            ]
        )
        result = response.choices[0].message.content.strip()
        logger.info("Paragraph extracted")
        return result
    except Exception as e:
        logger.error(f"Error calling OpenAI API: {e}")
        return "Error in extracting relevant information."


async def fetch_content_from_url(url: str) -> Optional[str]:
    logger.info(f"Fetching content from URL: {url}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as response:
                if response.status != 200:
                    logger.warning(f"Failed to fetch content from {url}. Status code: {response.status}")
                    return None

                if url.endswith('.pdf'):
                    logger.info(f"PDF detected: {url}")
                    content = await response.read()
                    return await extract_text_from_pdf(content)

                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                # Extract all text from the page
                text = soup.get_text(separator=' ', strip=True)
                logger.info(f"Fetched {len(text)} characters of text from {url}")
                return text
    except Exception as e:
        logger.error(f"An error occurred while fetching {url}: {e}")
        return None


async def extract_text_from_pdf(pdf_content: bytes) -> str:
    logger.info("Extracting text from PDF")
    pdf_reader = PyPDF2.PdfReader(BytesIO(pdf_content))
    text = ""
    for page in pdf_reader.pages:
        text += page.extract_text() + "\n"
    logger.info(f"Extracted {len(text)} characters from PDF")
    return text


def save_to_csv(filename: str, data: List[Tuple[str, str]]):
    logger.info(f"Saving data to CSV: {filename}")
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Link", "Relevant Paragraph"])
        writer.writerows(data)
    logger.info(f"Data saved to {filename}")


async def main():
    logger.info("Starting web scraper")
    query = ' "Cannabis" "Europe"'
    #add timeframe

    results = []

    all_links = await get_google_search_results(query, num_results=NUM_ARTICLES)

    for link in all_links:
        content = await fetch_content_from_url(link)
        if content:
            relevant_paragraph = await extract_relevant_paragraph(content, query)
            results.append((link, relevant_paragraph))
            print(f"\nLink: {link}")
            print(f"Relevant Paragraph:\n{relevant_paragraph}\n")

    if results:
        logger.info(f"Found relevant information from {len(results)} links")
        save_to_csv("search_results.csv", results)
        print("\nData has been written to 'search_results.csv'.")
    else:
        logger.warning("Could not find any relevant information")
        print("\nCould not find any relevant information.")

    logger.info("Web scraper finished")

if __name__ == "__main__":
    asyncio.run(main())