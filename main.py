import os
import csv
import sys
import PyPDF2
import logging
import asyncio
import aiohttp
import datetime
import traceback
import urllib.parse
from bs4 import BeautifulSoup
from io import BytesIO
from typing import List, Tuple, Optional
from dotenv import load_dotenv
from openai import AsyncOpenAI
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.errors import HttpError

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

# Call this function at the start of your script
logger = setup_logging()


# Load environment variables
load_dotenv()

# Initialize OpenAI API client
client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Google Sheets setup
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

async def get_google_search_results(query: str, start_date: str, end_date: str, num_results: int = 10) -> List[str]:
    try:
        logger.info(f"Searching Google for query: {query}")
        query_encoded = urllib.parse.quote_plus(f"{query} after:{start_date} before:{end_date}")
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
                return results[:num_results]
    except Exception as e:
        logger.error(f"An error occurred while searching Google: {e}")
        return []


async def extract_relevant_paragraph(text: str, query: str, instructions: str) -> str:
    logger.info("Extracting relevant paragraph with OpenAI")
    if instructions == None or instructions == "":
        message = [
                {"role": "system", "content": "You are an AI that extracts relevant economic data from text."},
                {"role": "user",
                 "content": f"Extract a paragraph containing relevant data matching this query: {query}. If no relevant information is found, respond with 'No relevant information found.'\n\nText: {text[:4000]}"}
            ]
    else:
        message = [
                {"role": "system", "content": "You are an AI that extracts relevant economic data from text."},
                {"role": "user",
                 "content": f"Extract a paragraph containing relevant data matching this query: {query}. Follow these instructions: {instructions}. If no relevant information is found, respond with 'No relevant information found.'\n\nText: {text[:4000]}"}
            ]
    try:
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=message
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


def save_to_csv(filename: str, data: List[List[str]]):
    file_exists = os.path.isfile(filename)
    existing_data = set()

    # Read existing data if file exists
    if file_exists:
        with open(filename, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader, None)  # Skip header
            existing_data = set(tuple(row) for row in reader)

    # Open file in append mode if it exists, otherwise in write mode
    mode = 'a' if file_exists else 'w'
    with open(filename, mode=mode, newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        # Write header if file is new
        if not file_exists:
            writer.writerow(["Keywords", "Link", "Relevant Paragraph"])
        
        # Write new, non-duplicate data
        new_rows = 0
        for row in data:
            if tuple(row) not in existing_data:
                writer.writerow(row)
                new_rows += 1
        
    logger.info(f"Added {new_rows} new rows to {filename}")

async def update_in_sheets(service, data: List[List[str]]):
    logger.info("Updating Google Sheets")
    try:
        values = [row + [datetime.datetime.now().isoformat()] for row in data]
        body = {'values': values}
        result = service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID, 
            range=f"{RESULT_SHEET_NAME}!A:D",  # Specify columns A through D
            valueInputOption='RAW', 
            insertDataOption='INSERT_ROWS',  # Ensure new rows are inserted
            body=body).execute()
        logger.info(f"Updated {result.get('updates').get('updatedCells')} cells in Google Sheets")
    except Exception as e:
        logger.error(f"An error occurred while updating Google Sheets: {e}")

async def main():
    logger.info("Starting web scraper")

    try:
        service = get_google_sheets_service()
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=SHEET_NAME).execute()
        rows = result.get('values', [])[1:]

        for index, row in enumerate(rows, start=2):
            results = []
            try:
                keywords, instructions, start_date, end_date, paragraph_count = row[:5]
                paragraph_count = int(paragraph_count)
                keywords = keywords.replace('\'', '')

                all_links = await get_google_search_results(keywords, start_date, end_date, num_results=paragraph_count * 2)

                for link in all_links:
                    if (len(results) == paragraph_count):
                        break
                    content = await fetch_content_from_url(link)
                    if content:
                        relevant_paragraph = await extract_relevant_paragraph(content, keywords, instructions)                        
                        results.append([keywords, link, relevant_paragraph.replace('\n', ' ')])
                        print(f"\nLink: {link}")
                        print(f"Relevant Paragraph:\n{relevant_paragraph}\n")
            except Exception as e:
                logger.error(f"Error processing row {index}: {e}")
            
            if results:
                logger.info(f"Found relevant information from {len(results)} links")
                await update_in_sheets(service, results)
                save_to_csv("search_results.csv", results)
                print("\nData has been written to 'search_results.csv'.")
            else:
                logger.warning("Could not find any relevant information")   
    except Exception as e:
        logger.critical(f"Critical error in main function: {str(e)}")
        logger.critical(traceback.format_exc())

    logger.info("Web scraper finished")

if __name__ == "__main__":
    asyncio.run(main())