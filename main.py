import os
import csv
import sys
import json  # For JSON handling
import base64
import openai
import PyPDF2
import logging
import asyncio
import aiohttp
import requests
import traceback
import urllib.parse
from io import BytesIO
from openai import OpenAI
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from typing import List, Optional
from pydantic import BaseModel, Field
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

# ---------------------- Setup Logging ----------------------

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Define log file path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(script_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, 'scraper.log')

    # File handler (logs to file)
    file_handler = logging.FileHandler(log_file_path)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    # Console handler (logs to screen)
    console_handler = logging.StreamHandler(sys.stdout)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    # Add both handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"Log file is being written to: {log_file_path}")

    return logger

# Initialize logging
logger = setup_logging()

# ---------------------- Load Environment Variables ----------------------

load_dotenv()

# Initialize OpenAI API client
client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),
)
openai.api_key = os.getenv("OPENAI_API_KEY")
ZYTE_API_KEY = os.getenv("ZYTE_API_KEY")

# ---------------------- Google Sheets Setup ----------------------

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1eT4ATuwD0PjWAGsGRQ7cGnh5XTN1-WW_8B9BNuiXgiI'
SHEET_NAME = 'Sheet2'
RESULT_SHEET_NAME = 'Sheet1'

def get_google_sheets_service(force_new_token=False):
    creds = None
    if os.path.exists('token.json') and not force_new_token:
        try:
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        except Exception as e:
            logger.error(f"Error reading token.json: {str(e)}")
            os.remove('token.json')
            logger.info("Removed invalid token.json file")

    if not creds or not creds.valid or force_new_token:
        if creds and creds.expired and creds.refresh_token and not force_new_token:
            try:
                creds.refresh(Request())
                logger.info("Successfully refreshed the token")
            except Exception as e:
                logger.error(f"Error refreshing credentials: {str(e)}")
                creds = None

        if not creds:
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
                logger.info("Generated new token through authorization flow")
            except Exception as e:
                logger.error(f"Error in authorization flow: {str(e)}")
                raise

        with open('token.json', 'w') as token:
            token.write(creds.to_json())
            logger.info("Saved new token.json file")

    try:
        service = build('sheets', 'v4', credentials=creds)
        logger.info("Successfully created Google Sheets service")
        return service
    except HttpError as error:
        logger.error(f"An error occurred while building the service: {error}")
        if "invalid_grant" in str(error) or "invalid_scope" in str(error):
            logger.info("Token seems to be invalid. Attempting to generate a new one.")
            return get_google_sheets_service(force_new_token=True)
        raise

# ---------------------- Scraped URLs Management ----------------------

SCRAPED_URLS_FILE = 'scraped_urls.json'

def load_scraped_urls() -> set:
    if os.path.exists(SCRAPED_URLS_FILE):
        try:
            with open(SCRAPED_URLS_FILE, 'r', encoding='utf-8') as f:
                scraped = set(json.load(f))
                logger.info(f"Loaded {len(scraped)} scraped URLs from {SCRAPED_URLS_FILE}")
                return scraped
        except Exception as e:
            logger.error(f"Error loading scraped URLs: {e}")
    return set()

def save_scraped_urls(scraped_urls: set):
    try:
        with open(SCRAPED_URLS_FILE, 'w', encoding='utf-8') as f:
            json.dump(list(scraped_urls), f)
            logger.info(f"Saved {len(scraped_urls)} scraped URLs to {SCRAPED_URLS_FILE}")
    except Exception as e:
        logger.error(f"Error saving scraped URLs: {e}")

# Load scraped URLs at the start
scraped_urls = load_scraped_urls()

# ---------------------- Search Engine Configuration ----------------------

# Rate limiting configurations
GOOGLE_RATE_LIMIT = 2  # seconds between Google requests
BING_RATE_LIMIT = 2     # seconds between Bing requests

# Semaphore to control rate limiting
google_semaphore = asyncio.Semaphore(1)
bing_semaphore = asyncio.Semaphore(1)

# ---------------------- Search Functions ----------------------
async def get_google_search_results(query: str, start_date: str, end_date: str,
                                    num_results: int = 10, scraped_urls: set = set(),
                                    max_pages: int = 10) -> List[str]:
    """
    Fetch Google search results while excluding already scraped URLs.
    Implements pagination to fetch more results if needed.
    Falls back to Bing search if Google fails.
    """
    start_date = start_date.replace('-', '/')
    end_date = end_date.replace('-', '/')
    query_parts = query.lower().split()
    query_edited = '+'.join(urllib.parse.quote(part) for part in query_parts)
    try:
        logger.info(f"Searching Google for query: {query}")
        # query_encoded = urllib.parse.quote_plus(f"{query} after:{start_date} before:{end_date}")
        new_results = []
        current_page = 0

        while len(new_results) < num_results:
            query_encoded = f"%27{query_edited}%27&sca_esv=47b2934ea174e9e2&rlz=1C1CHBD_en-GBPK1108PK1108&tbs=cdr:1,cd_min:{start_date},cd_max:{end_date}&ei=95YaZ5uUBbnbptQPi53AqQc&start={current_page*10}"
            async with google_semaphore:
                await asyncio.sleep(GOOGLE_RATE_LIMIT)  # Rate limiting

                # start = current_page * 10  # Google uses 'start' parameter for pagination
                url = f"https://www.google.com/search?q={query_encoded}" #&num=10&start={start}"
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                                  'Chrome/85.0.4183.102 Safari/537.36'
                }

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers) as response:
                        if response.status != 200:
                            logger.error(f"Google search failed with status code: {response.status}")
                            raise Exception("Google search failed")

                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        results = []
                        for item in soup.find_all('div', class_='yuRUbf'):  # This class may change
                            link = item.find('a', href=True)
                            if link and link['href'].startswith('http'):
                                results.append(link['href'])

                        # Filter out already scraped URLs
                        filtered = [link for link in results if link not in scraped_urls]
                        new_results.extend(filtered)

                        logger.info(f"Fetched {len(filtered)} new links from Google page {current_page}")

            current_page += 1

        logger.info(f"Total new Google search results found: {len(new_results)}")
        return new_results[:num_results]
    except Exception as e:
        logger.error(f"Google search failed: {e}")
        logger.info("Falling back to Bing search")
        # Fallback to Bing search
        return await get_bing_search_results(query, start_date, end_date, num_results, scraped_urls, max_pages)

async def get_bing_search_results(query: str, start_date: str, end_date: str,
                                  num_results: int = 10, scraped_urls: set = set(),
                                  max_pages: int = 10) -> List[str]:
    """
    Fetch Bing search results while excluding already scraped URLs.
    Implements pagination to fetch more results if needed.
    """
    try:
        logger.info(f"Searching Bing for query: {query}")
        query_encoded = urllib.parse.quote_plus(f"{query} after:{start_date} before:{end_date}")
        new_results = []
        current_page = 0

        while current_page < max_pages:
            async with bing_semaphore:
                await asyncio.sleep(BING_RATE_LIMIT)  # Rate limiting

                first = current_page * 10 + 1  # Bing uses 'first' parameter for pagination
                url = f"https://www.bing.com/search?q={query_encoded}&count=10&first={first}"
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                                  'Chrome/85.0.4183.102 Safari/537.36'
                }

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers) as response:
                        if response.status != 200:
                            logger.error(f"Bing search failed with status code: {response.status}")
                            break  # Stop trying Bing if a page fails

                        html = await response.text()  
                        soup = BeautifulSoup(html, 'html.parser')
                        results = [item['href'] for item in soup.find_all('a', href=True)
                                   if item['href'].startswith('http')]

                        # Filter out already scraped URLs and non-organic links
                        filtered = []
                        for link in results:
                            if link not in scraped_urls and not any(domain in link for domain in ['.jpg', '.png', '.pdf', '.gif']):
                                filtered.append(link)

                        new_results.extend(filtered)

                        logger.info(f"Fetched {len(filtered)} new links from Bing page {current_page + 1}")

                        if len(new_results) >= num_results:
                            break

                current_page += 1

        logger.info(f"Total new Bing search results found: {len(new_results)}")
        return new_results[:num_results]
    except Exception as e:
        logger.error(f"Bing search failed: {e}")
        return []

# ---------------------- Content Fetching Functions ----------------------

async def fetch_content_with_zyte(url: str) -> Optional[str]:
    logger.info(f"Fetching content from URL: {url}")
    try:
        api_response = requests.post(
            "https://api.zyte.com/v1/extract",
            auth=(ZYTE_API_KEY, ""),
            json={
                "url": url,
                "httpResponseBody": True,
            },
        )
        api_response.raise_for_status()
        response_json = api_response.json()
        
        if "httpResponseBody" not in response_json:
            logger.error("Error: 'httpResponseBody' not found in API response")
            logger.error(f"API Response: {response_json}")
            return None
        
        http_response_body: bytes = base64.b64decode(response_json["httpResponseBody"])
        
        if url.lower().endswith('.pdf'):
            logger.info(f"PDF detected: {url}")
            return await extract_text_from_pdf(http_response_body)
        
        soup = BeautifulSoup(http_response_body, 'html.parser')
        text = soup.get_text(separator=' ', strip=True)
        logger.info(f"Fetched {len(text)} characters of text from {url}")
        return text
    except requests.RequestException as e:
        logger.error(f"Request error while fetching {url}: {e}")
    except ValueError as e:
        logger.error(f"JSON decoding error for {url}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while fetching {url}: {e}")
    return None

async def fetch_content_from_url(url: str) -> Optional[str]:
    """
    Asynchronously fetch content from a URL, handling both HTML and PDF content.
    
    Args:
        url (str): The URL to fetch content from
        
    Returns:
        Optional[str]: The extracted text content, or None if the fetch fails
    """
    logger.info(f"Fetching content from URL: {url}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                content = await response.read()
                
                if url.lower().endswith('.pdf'):
                    logger.info(f"PDF detected: {url}")
                    return await extract_text_from_pdf(content)
                
                # Attempt to decode the content with UTF-8, fallback to ISO-8859-1
                try:
                    html_content = content.decode('utf-8')
                except UnicodeDecodeError:
                    html_content = content.decode('iso-8859-1')
                
                soup = BeautifulSoup(html_content, 'html.parser')
                text = soup.get_text(separator=' ', strip=True)
                logger.info(f"Fetched {len(text)} characters of text from {url}")
                return text
                
    except aiohttp.ClientError as e:
        logger.error(f"Request error while fetching {url}: {e}")
    except UnicodeDecodeError as e:
        logger.error(f"Decoding error for {url}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while fetching {url}: {e}")
    
    return None

async def extract_text_from_pdf(pdf_content: bytes) -> str:
    logger.info("Extracting text from PDF")
    try:
        pdf_reader = PyPDF2.PdfReader(BytesIO(pdf_content))
        text = ""
        for page in pdf_reader.pages:
            extracted = page.extract_text()
            if extracted:
                text += extracted + "\n"
        logger.info(f"Extracted {len(text)} characters from PDF")
        return text
    except Exception as e:
        logger.error(f"Error extracting text from PDF: {e}")
        return ""
        
# ---------------------- OpenAI Paragraph Extraction ----------------------
class ParagraphResponse(BaseModel):
    content: Optional[str] = Field(description="Extract a paragraph which is most relevant to the given Query. Return only if the paragraph(content) is from an article otherwise empty")  # The extracted paragraph
    title: Optional[str] = Field(description="The title of the article")  # The title of the article
    subtitle: Optional[str] = Field(description="The subtitle of the article")  # The subtitle of the article
    score: Optional[float] = Field(description="Return relevancy score out of 100 how much the paragraph you extracted is relevant to the given query")  # The relevancy score of the paragraph
    keywords: Optional[List[str]] = Field(description='["Comma-separated list of keywords (array of strings)"]')  # The keywords of the article
    category: Optional[str] = Field(description="The main category of the article")  # The category of the article
    date: Optional[str] = Field(description="The publishing date (ISO 8601 format, e.g., \'2024-03-15T14:30:00\')")  # The publishing date of the article
    source: Optional[str] = Field(description="Return the source of the article, eg: the authority who released the info or organization which did the resarch etc like these: U.S. Food and Drug Administration (FDA), U.S. Department of Agriculture (USDA), Canadian Cannabis Growers Association, Statistics Canada, Oregon Liquor Control Commission. These are the authorities/organizations")  # The source of the extracted paragraph
    numeric_value: Optional[float] = Field(description='"A relevant numeric value or quantity from the content (number)"')  # Any numeric value in the paragraph
    unit: Optional[str] = Field(description="The unit of measurement for the extracted numeric value. For example, if '4.7 billion' is found in the article, return 'billion'. For '$14 million', return 'million'. For '73.5%', return 'percent'. Always return the unit in words, not symbols. If there's no unit (e.g., just a plain number), return 'count' or leave it empty.")
    type: Optional[str] = Field(description='"The type of the numeric value (e.g., \'revenue\', \'users\', \'sales\') (string)",\n')  # The type of the numeric value
    country: Optional[str] = Field(description="The primary country related to the article")  # Specific country related to the article
    location: Optional[str] = Field(description="Any specific location within the country mentioned in the article")  # Any Specific location within the country mentioned in the article
    author: Optional[str] = Field(description="The author of the article")  # The author of the article
    references: Optional[List[str]] = Field(description='["Any references or citations in the article"]')  # Any references or citations in the article
    
def extract_relevant_data(text: str, query: str, instructions: str) -> ParagraphResponse:
    logger.info("Extracting relevant Data with OpenAI")
    system_prompt = (
        "You are an advanced data extraction assistant. Your task is to read the provided text thoroughly, "
        "analyze each paragraph, and extract a paragraph and information relevant to the given query and instructions. "
        "Focus particularly on paragraphs that include numerical data such as Users, Sales, Revenues, Turnover, "
        "Stores, Dispensaries, Licenses, Pounds, Ounces, or similar metrics. Your goal is to extract the most "
        "pertinent information that aligns with the given criteria and structure it according to the specified format."
    )
        
    user_prompt = (
        f"Query: '{query}'\n"
        f"Instructions: {instructions if instructions else 'No specific instructions provided.'}\n\n"
        f"Text: {text[:25000]}\n\n"
        "Extract and return the information in JSON format:\n"
        "Ensure all fields are present. Use '--' for unavailable information. Do not include any explanations or additional text outside the JSON structure."
    )

    try:
        response = client.beta.chat.completions.parse(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format=ParagraphResponse,        
        )
        result = response.choices[0].message.parsed
        if result.content is None:
            return check_again_in_openai(text, query, instructions)
        return result
    except Exception as e:
        logger.error(f"Error calling OpenAI API: {e}")
        return ParagraphResponse(
            content='--',
            title='--',
            subtitle='--',
            score=0.0,
            keywords=[],
            category='--',
            date='--',
            source='--',
            numeric_value=None,
            unit='--',
            type='--',
            country='--',
            location='--',
            author='--',
            references=[]
        )

def check_again_in_openai(text: str, query: str, instructions: str) -> ParagraphResponse:
    logger.info("Checking again with OpenAI")
    system_prompt = (
        "You are an advanced data extraction assistant. Your task is to read the provided text thoroughly, "
        "analyze each paragraph, and extract information relevant to the given query and instructions. "
        "Main task is to extract a paragraph from the given text that contains information relevant to the given query and instructions. "
        "Focus particularly on paragraphs that include numerical data such as Users, Sales, Revenues, Turnover, "
        "Stores, Dispensaries, Licenses, Pounds, Ounces, or similar metrics. Your goal is to extract the most "
        "pertinent information that aligns with the given criteria and structure it according to the specified format."
        "I am 100% sure that a relevant paragraph exists in the text. Please extract the most relevant paragraph."
    )
    user_prompt = (
        f"Query: '{query}'\n"
        f"Instructions: {instructions if instructions else 'No specific instructions provided.'}\n\n"
        f"Text: {text[:25000]}\n\n"
        "Extract and return the information in JSON format:\n"
        "Ensure all fields are present. Use '--' for unavailable information. Do not include any explanations or additional text outside the JSON structure."
    )

    try:
        response = client.beta.chat.completions.parse(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format=ParagraphResponse,        
        )
        result = response.choices[0].message.parsed
        if result.content is None:
            return None
        return result
    except Exception as e:
        logger.error(f"Error calling OpenAI API: {e}")
        # Return a proper ParagraphResponse object instead of a string
        return ParagraphResponse(
            content='--',
            title='--',
            subtitle='--',
            score=0.0,
            keywords=[],
            category='--',
            date='--',
            source='--',
            numeric_value=None,
            unit='--',
            type='--',
            country='--',
            location='--',
            author='--',
            references=[]
        )

# ---------------------- CSV Saving Function ----------------------

def save_to_csv(filename: str, data: List[List[str]]):
    file_exists = os.path.isfile(filename)
    existing_data = set()
    header_exists = False

    # Read existing data if file exists
    if file_exists:
        with open(filename, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            first_row = next(reader, None)
            if first_row == ["Keywords", "Link", "Relevant Paragraph", 'Title', 'Relevancy Score', 'Keywords', 'Category', 'Date', 'Source', 'Numeric Value', 'Unit', 'Type', 'Country', 'Location', 'Author', 'References']:
                header_exists = True
            else:
                file.seek(0)  # Reset file pointer to beginning
            existing_data = set(tuple(row) for row in reader)

    # Open file in append mode if it exists, otherwise in write mode
    mode = 'a' if file_exists else 'w'
    with open(filename, mode=mode, newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        # Write header if file is new or doesn't have the header
        if not file_exists or not header_exists:
            writer.writerow(["Keywords", "Link", "Relevant Paragraph", 'Title', 'Relevancy Score', 'Keywords', 'Category', 'Date', 'Source', 'Numeric Value', 'Unit', 'Type', 'Country', 'Location', 'Author', 'References'])

        # Write new, non-duplicate data
        new_rows = 0
        for row in data:
            if tuple(row) not in existing_data:
                writer.writerow(row)
                new_rows += 1

    print(f"Added {new_rows} new rows to {filename}")
    logger.info(f"Added {new_rows} new rows to {filename}")

# ---------------------- Google Sheets Update Function ----------------------

async def update_in_sheets(service, data: List[List[str]]):
    logger.info("Updating Google Sheets")
    try:
        values = [row for row in data]
        body = {'values': values}
        result = service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{RESULT_SHEET_NAME}!A:D",
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body=body).execute()
        updated_cells = result.get('updates', {}).get('updatedCells', 0)
        logger.info(f"Updated {updated_cells} cells in Google Sheets")
    except HttpError as e:
        logger.error(f"HttpError occurred while updating Google Sheets: {e}")
    except Exception as e:
        logger.error(f"An error occurred while updating Google Sheets: {e}")

# ---------------------- Google Sheets Counter Update Function ----------------------

async def update_scraped_counter(service, row_index: int, new_count: int):
    logger.info(f"Updating scraped counter for row {row_index} to {new_count}")
    try:
        sheet = service.spreadsheets()
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!G{row_index}",  # Assuming Column G is the scraped counter
            valueInputOption="RAW",
            body={"values": [[new_count]]}
        ).execute()
        logger.info(f"Updated scraped counter for row {row_index - 1} to {new_count}")
    except Exception as e:
        logger.error(f"Error updating scraped counter for row {row_index - 1}: {e}")

# ---------------------- Main Scraper Function ----------------------

async def main():
    logger.info("Starting web scraper")

    try:
        service = get_google_sheets_service()
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=SHEET_NAME).execute()
        rows = result.get('values', [])[1:]  # Skip header

        for index, row in enumerate(rows, start=2):  # Start at 2 to account for header
            try:
                # Unpack row data with default values if missing
                keywords = row[0] if len(row) > 0 else ""
                instructions = row[1] if len(row) > 1 else ""
                start_date = row[2] if len(row) > 2 else ""
                end_date = row[3] if len(row) > 3 else ""
                paragraph_count = row[4]
                scraped_flag =  row[5] if len(row) > 5 else "FALSE"
                scraped_counter = int(row[6]) if len(row) > 6 else 0  

                print(f"\n\nRow {index - 1}: {keywords}, {instructions}, {start_date}, {end_date}, {paragraph_count}, {scraped_flag}, {scraped_counter}\n\n")
                if scraped_flag.upper() == "TRUE":
                    logger.info(f"Row {index - 1} already fully scraped. Skipping.")
                    continue
                
                if paragraph_count == "0" or not paragraph_count:
                    logger.info("No Number of Paragraphs to Extract Provided. Skipping....")
                    continue

                try:
                    paragraph_count = int(paragraph_count)
                except ValueError:
                    logger.warning(f"Invalid paragraph count in row {index - 1}. Skipping.")
                    continue

                keywords = keywords.replace('\'', '').strip()

                # Calculate remaining paragraphs to scrape
                remaining_paragraphs = paragraph_count - scraped_counter
                if remaining_paragraphs <= 0:
                    logger.info(f"Row {index - 1} has already scraped the required number of paragraphs.")
                    # Optionally, mark as fully scraped
                    sheet.values().update(
                        spreadsheetId=SPREADSHEET_ID,
                        range=f"{SHEET_NAME}!F{index}",
                        valueInputOption="RAW",
                        body={"values": [["TRUE"]]}
                    ).execute()
                    logger.info(f"Marked row {index - 1} as fully scraped in Google Sheets")
                    continue

                # Fetch search results in batches of 10 pages
                page = 1
                while remaining_paragraphs > 0:
                    links = await get_google_search_results(
                    # links = await get_bing_search_results(
                        keywords,
                        start_date,
                        end_date,
                        num_results=10,  # Fetch enough to cover multiple batches
                        scraped_urls=scraped_urls,
                        max_pages=page
                    )
                    if not links:
                        continue
                    
                    processed_paragraphs = 0
                    for link in links:
                        if link in scraped_urls:
                            logger.info(f"URL already scraped: {link}. Skipping.")
                            continue

                        content = await fetch_content_from_url(link)
                    
                        if not content:
                            content = await fetch_content_with_zyte(link)
                            if not content:
                                logger.error(f"Failed to fetch content from URL: {link}. Skipping.")
                                continue
                            
                        relevant_data = extract_relevant_data(content, keywords, instructions) 

                        if relevant_data is None:
                            print('Not found any Data')
                            # Add to scraped URLs and save immediately
                            scraped_urls.add(link)
                            save_scraped_urls(scraped_urls)
                            continue
                
                        paragraph = relevant_data.content
                        title = relevant_data.title
                        score = relevant_data.score
                        new_keywords = ', '.join(relevant_data.keywords) if relevant_data.keywords else "-"
                        category = relevant_data.category
                        date = relevant_data.date
                        source = relevant_data.source
                        numeric_value = relevant_data.numeric_value
                        unit = relevant_data.unit
                        type = relevant_data.type
                        country = relevant_data.country
                        location = relevant_data.location
                        author = relevant_data.author

                        data_row = [keywords, link, paragraph.replace('\n', ' '), title, score, new_keywords, category, date, source, numeric_value, unit, type, country, location, author]

                        # Save to CSV
                        save_to_csv("search_results.csv", [data_row])

                        # Update Google Sheets
                        await update_in_sheets(service, [data_row])

                        # Add to scraped URLs and save immediately
                        scraped_urls.add(link)
                        processed_paragraphs += 1
                        remaining_paragraphs -= 1
                        page += 1
                        save_scraped_urls(scraped_urls)
                        scraped_counter += 1
                        await update_scraped_counter(service, index, scraped_counter)
                    
                        # If the required number of paragraphs has been scraped, mark the row as scraped
                        if scraped_counter >= paragraph_count:
                            sheet.values().update(
                                spreadsheetId=SPREADSHEET_ID,
                                range=f"{SHEET_NAME}!F{index}",
                                valueInputOption="RAW",
                                body={"values": [["TRUE"]]}
                            ).execute()
                            logger.info(f"Marked row {index} as fully scraped in Google Sheets")

                        logger.info(
                            f"Processed {processed_paragraphs} paragraphs for row {index}. Total scraped: {scraped_counter}/{paragraph_count}")
        
                        if remaining_paragraphs <= 0:
                            break
            except Exception as e:
                logger.error(f"Error processing row {index}: {e}")
                logger.error(traceback.format_exc())
    except Exception as e:
        logger.critical(f"Critical error in main function: {str(e)}")
        logger.critical(traceback.format_exc())

    logger.info("Web scraper finished")

# ---------------------- Entry Point ----------------------

if __name__ == "__main__":
    asyncio.run(main())
