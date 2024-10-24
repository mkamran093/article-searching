from base64 import b64decode
import os
from typing import List, Optional
import urllib
from openai import OpenAI
from pydantic import BaseModel, Field
import requests
import json
from dotenv import load_dotenv

load_dotenv()
client = OpenAI()

class ParagraphResponse(BaseModel):
    content: Optional[str] = Field(description="Extract a paragraph which is most relevant to the given Query.")  # The extracted paragraph
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

query = "THC Germany Sales $"
text = "Germany imported a record amount of cannabis for medical and scientific use in 2023, as international businesses looked for sales opportunities in Europe’s largest federally regulated medical marijuana market. In 2023, Germany imported 31,398 kilograms (34.6 tons) of cannabis products, according to the newest data from the country’s Federal Institute for Drugs and Medical Devices (BfArM). That’s a 26.2% increase over the 24,876 kilograms imported the previous year. In previous years, Germany imported: 24,876 kilograms (27.4 tons) of cannabis in 2022, up 19.8% over 2021. 20,771 kilograms (22.9 tons) in 2021, a 77% surge over the previous year. 11,746 kilograms (12,8 tons) in 2020, 46% more than in 2019. 8,057 kilograms (8.9 tons) in 2019, which was 80% more than 2018’s total cannabis imports. An unknown amount of those imports is reexported to other European Union countries every year, but industry experts say the data still paints a picture of an industry that is growing fast. End of the quota system Peter Homberg, a partner at Dentons in Germany and head of the law firm's European Cannabis Group, said the country imports so much cannabis because of insufficient domestic cultivation, which stemmed from a now-scrapped cultivation quota system. In 2019, only three companies were chosen to cultivate medical marijuana in Germany after a lengthy quota application process. No one else was allowed to grow medical cannabis for commercial purposes. Together, the cultivators were allowed to produce only 10,400 kilograms of cannabis over a period of four years. Meeting demand in excess of the limited domestic cultivation meant companies had to import cannabis from abroad. Germany’s reliance on imports might begin to wane in the years to come, however. Germany’s new cannabis law scrapped the quota system. In its place, companies may now apply for a permit to grow medical marijuana from the Cannabis Agency. “The slightly more flexible conditions for the cultivation of medical cannabis” as a result of the new law “will make imports from abroad less necessary,” Homberg said. Imports, but for how long? Germany has been one of the largest importers of medical cannabis for years, a relief for marijuana exporters that generally struggle to access meaningful import markets. Canada, the largest federally regulated medical marijuana market in the world, has blocked the commercial import of medical cannabis for years, pushing prospective exporters to look for buyers in countries such as Australia, Brazil, Germany and Israel. However, Germany’s cannabis law, which took effect April 1, removed marijuana from its classification as a narcotic. That means patients seeking access to medical cannabis in Germany will no longer require a narcotic prescription form. Standard prescriptions will be sufficient, and that’s expected to stoke demand in the medical cannabis market. 'Huge growth potential' for commercial cultivation Constantin von der Groeben, managing director of the German company Demecan, told MJBizDaily that, as of April 1, companies can apply for a cultivation license. “We foresee a huge growth potential for our own cultivation,” he said, indicating Demecan can apply to increase cultivation beyond the previous maximum production quota. However, Germany is still expected to need significant cannabis imports in the near- to midterm. “It depends strongly on the ramp-up of domestic cultivation,” Von der Groeben said of how long Germany would rely on imports. The Demecan executive cited Article 21 of the United Nations' Single Convention on Narcotic Drugs, which he said implies that if and when the demand for cannabis can be filled domestically, no imports would be required or allowed. “But this is probably still a few years out,” he said. Canada still top supplier Canada remained by far the top supplier to the German market in 2023, followed by Portugal and the Netherlands in a distant second and third, respectively. Roughly 15,600 kilograms of cannabis was shipped from Canada to Germany in 2023, representing approximately 50% of Germany’s marijuana imports for medical and scientific use as well as reexporting. In 2022, Canadian shipments accounted for less than 40% of Germany’s total imports.  Matt Lamers can be reached at matt.lamers@mjbizdaily.com."  # Limiting to first 8000 characters to comply with API limits"

system_prompt = (
    "You are an advanced data extraction assistant. Your task is to read the provided text thoroughly, "
    "analyze each paragraph, and extract a paragraph and information relevant to the given query and instructions. "
    "Focus particularly on paragraphs that include numerical data such as Users, Sales, Revenues, Turnover, "
    "Stores, Dispensaries, Licenses, Pounds, Ounces, or similar metrics. Your goal is to extract the most "
    "pertinent information that aligns with the given criteria and structure it according to the specified format."
)

user_prompt = (
    f"Query: '{query}'\n"
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
    print(result)
except Exception as e:
        print(f"Error calling OpenAI API: {e}")