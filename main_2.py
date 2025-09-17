from langchain_llm7 import ChatLLM7
from langchain_core.messages import HumanMessage
from langchain.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field
from typing import List, Optional, Union
import json
import re
import pandas as pd
import supabase
from pprint import pprint
from supabase import create_client, Client
import supabase
from dotenv import load_dotenv
import string
import re
import os
from time import sleep
load_dotenv()

supabase_key: str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inp2ZWN5bW1yYXF0cWp0ZXVvb2ZlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTY4Mjk4NjksImV4cCI6MjA3MjQwNTg2OX0.x0wh7O5xfzhqTj4ag4i4sGJ8FBwISRBkM2Wrqzz89gs"
supabase_url: str = "https://zvecymmraqtqjteuoofe.supabase.co"
print(supabase_key )
supabase: Client = create_client(supabase_url, supabase_key)
df = pd.read_csv("chunk_2.csv")
df['Volltext'] = df['Volltext'].astype(str)

# Create a regex pattern: match all punctuation except /
punctuation_to_remove = f"[{re.escape(string.punctuation.replace('/', ''))}]"

# Apply replacement
df['Volltext'] = df['Volltext'].str.replace(punctuation_to_remove, '', regex=True)
df['Volltext'] = df['Volltext'].str.replace(r"\\", "", regex=True) 
df['Volltext'] = df['Volltext'].str.replace(r"\\[ntrbfv]", " ", regex=True)
df['Volltext'] = df['Volltext'].str.replace(r"\s+", " ", regex=True).str.strip()

  
llm = ChatLLM7(
    model="gpt-4.1-nano-2025-04-14",
    api_key = 'PpdJBM+4g8lR4jSDGP+6kQF4G2oylkHiuUP17fyXQQGhH8zsVAQIfQjONWFhk5rL6vqMlwMOfNRTLHTTijF6MQWCEwcoocLlmJOeMAAD0Rem1GqtXKfgm2JNyVQl',
    temperature=0.0,
    max_tokens=1000,
    stop=None,
    timeout=45
)

# -----------------------------
# Metadata extractor
# -----------------------------
def extract_metadata_with_schema(text: str):
    """Extract bibliographic metadata using LLM7 + enforced schema"""

    prompt = f"""
    You are an expert library cataloging assistant specialized in processing OCR texts of German library books.
The text may contain spelling mistakes, OCR artifacts, inconsistent formatting, missing punctuation, or misplaced information.


Your task is to:


1. Preserve the first number (signature number) at the start of the text exactly as it appears. Do not modify, remove, or alter this number under any circumstance.
2. Correct all OCR errors and spelling mistakes while keeping the text in its original German language.
3. Standardize formatting (consistent spacing, punctuation, capitalization) while ensuring no information is lost.
4. Keep all original information intact — do not summarize, omit, or add new information.
5. Ensure readability and accuracy while reflecting the original meaning as closely as possible.
6. Donot add puntuations

Return only the corrected text as output. Do not provide explanations, comments, or additional formatting beyond the corrected text.


Examples (Input -> Output):


Input:
"BrÅck, EBrÅck,E\tReichsgesetz Åber den Versicherungsvertragnebst dem zugehîrigen EinfÅhrungsgesetz. Vom 30.fiai 1908.7.*A. <2Q.bis 23.Taue,>Gattentagsche Sammlung Deutscher Reichsgesetze. Nr.83.BerlinrW.de Gruyter & Co. 1932. (VIII,563,56 S.)"


Output:
"Brück, E. Brück, E. Reichsgesetz über den Versicherungsvertrag nebst dem zugehörigen Einführungsgesetz. Vom 30. Mai 1908. 7. Auflage. (20. bis 23. Tausend) Guttentagsche Sammlung Deutscher Reichsgesetze. Nr. 83. Berlin: W. de Gruyter & Co. 1932. (VIII, 563, 56 S.)"


Input:
"BrÅck, ErnstBrÅck, Ernst. Das Priva tversictierungerechtMannheim:J.Benelieimer 1930.\t(35,819 S.)1771. A (Prof .Dr.) r/t"


Output:
"Brück, Ernst. Brück, Ernst. Das Privatversicherungsrecht. Mannheim: J. Bensheimer 1930. (35, 819 S.) 1771. A (Prof. Dr.)"


Input:
"Bruok.B\tit 0 t-?BrÅck,E und P Hager. Reichageeetz Åber denVersicherungsvertrag, nebst dem zugehîrigen EinfÅhrungs-geaetz vom 50.Mai 1906.5.*A.v.E.BrÅck.Guttentageche Sammlung deutscher Reichsgeeetze,Br.83.Berlin:W.de Gruyter & Co. 1926. (Z,5)"


Output:
"Brück, E. und P. Hager. Reichsgesetz über den Versicherungsvertrag nebst dem zugehörigen Einführungsgesetz vom 30. Mai 1906. 5. Auflage von E. Brück. Guttentagsche Sammlung Deutscher Reichsgesetze, Nr. 83. Berlin: W. de Gruyter & Co. 1926. (Z, 5)"
    {text}
Return only a JSON object strictly in this format:
{{"Volltext": "<corrected text>"}}

Do NOT include explanations, markdown, or extra text.

    """
    response = llm.invoke([HumanMessage(content=prompt)])
    content = response.content
    match = re.search(r"\{.*\}", content, re.DOTALL)
    if match:
        cleaned = match.group(0)
    else:
        cleaned = None

    print("LLM content:", cleaned)
    try:
        
        return cleaned
   
        
    except Exception as e:
        
        raise ValueError(f"Failed to parse response: {response.content}\nError: {e}")


import math

def clean_nans(data_list):
    cleaned = []
    for item in data_list:
        fixed = {k: (None if (isinstance(v, float) and math.isnan(v)) else v) for k, v in item.items()}
        cleaned.append(fixed)
    return cleaned
data_list = []
for index, column in df.iterrows():
    print(f"Processing row {index}")
    try:
        metadata = extract_metadata_with_schema(column['volltext'])
        metadata = json.loads(metadata)['Volltext']
        
        output = {
            "Titel_Autor": column['Titel_Autor'],
            "RASignatur": column['RASignatur'],
            "Volltext": metadata,
            "Autorenvorname": column['Autorenvorname']
        }
        print(output)
        data_list.append(output)

    except Exception as e:
        print(f"⚠️ Skipping row {index} due to error: {e}")
        continue   # Skip this row, move to the next

    if index % 10 == 0 and index > 0:
        try:
            cleaned_data = clean_nans(data_list)
            response = supabase.from_("Voltext").upsert(cleaned_data).execute()
            print(f"✅ Upserted batch at index {index}, response: {response}")
        except Exception as e:
            print(f"⚠️ Failed to upload batch at index {index}: {e}")
        finally:
            data_list = []  # Always reset after attempt
            sleep(2)
