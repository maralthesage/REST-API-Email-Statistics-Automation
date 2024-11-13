import requests
from requests.auth import HTTPBasicAuth
import json
import pandas as pd
import re
from creds import api


url = api["URL"]
stat_url = api["STAT_URL"]
client_id = api["API_KEY"]
secret = api["API_SECRET"]
listID = api["LIST_ID"]
words = api["WORDS"]

pattern = rf"({'|'.join(words)})_(.*?)_"
headers = {'Accept': 'application/hal+json'}

data = []

for id in listID:
    params = {
        'listIds': id,
        'afterId': 720000,
        'pageSize': 3000
    }
    
    # Make the GET request with query parameters
    response = requests.get(url, headers=headers, params=params, auth=HTTPBasicAuth(client_id, secret))

    # Check the response
    if response.status_code == 200:
        mailing = response.json()['_embedded']['inx:mailings'][-28:]
        mailingIds = {}
        names = []
        
        for item in mailing:
            mailingIds[item['id']] = item['name']
            
    else:
        print("Error:", response.status_code)
        

    for mailing,name in mailingIds.items():

        matches = re.findall(pattern, name)
        # Separate variables for words and phrases
        typ = [match[0] for match in matches]
        thema = [match[1] for match in matches]
        sources = ['responses','sendings']
        # Use a valid mailingId that is supported by the API
        params = {
            'mailingId': mailing  # Replace with a valid mailing ID
        }

        results = []
        for source in sources:

            # Make the GET request with query parameters
            response = requests.get(stat_url+source, headers=headers, params=params, auth=HTTPBasicAuth(client_id, secret))

            # Check the response
            if response.status_code == 200:
                stats = response.json()
                results.append(stats)
            else:
                print("Error:",id,mailing, response.status_code, response.text)
        try:
            # Attempt to extract the response and sending data from the API results
            response = pd.DataFrame(results[0])
            sending = pd.DataFrame(results[1])
            
            # Processing the 'sending' dataframe
            sending['startDate'] = pd.to_datetime(sending['startDate'], format='mixed')
            sending['startDate'] = sending['startDate'].dt.strftime('%d.%m.%Y')
            sending = sending.rename(columns={'recipientsCount': 'Empfänger brutto',
                                            'startDate': 'Datum',
                                            'deliveredMailsCount': 'Empfänger netto',
                                            'bounceCount': 'Rückläufer'})

            # Processing the 'response' dataframe
            response = response.rename(columns={'openingRecipients': 'Öffnungen',
                                                'clickingRecipients': 'unique Klicks',
                                                'unsubscribeClicks': 'Abmeldungen'})
            
            # Creating a new dataframe with the necessary columns
            df = pd.DataFrame([[id, mailing, sending['Datum'].values[0], thema[0], typ[0], 
                                sending['Empfänger brutto'].values[0], sending['Empfänger netto'].values[0],
                                response['Abmeldungen'].values[0], sending['Rückläufer'].values[0],
                                response['Öffnungen'].values[0], response['unique Klicks'].values[0]]],
                            columns=['ListId', 'MailingId', 'Datum', 'Thema', 'Typ', 'Empfänger brutto', 
                                    'Empfänger netto', 'Abmeldungen', 'Rückläufer', 'Öffnungen', 'unique Klicks'])

            # Appending the processed data to the main list
            data.append(pd.DataFrame(df))

        except (IndexError, KeyError, pd.errors.EmptyDataError) as e:
            # Handle specific errors, like missing data or invalid structure
            print(f"Error processing data for id {id}: {e}. Moving to the next item.")
        except Exception as e:
            # Catch-all for any other exceptions (like API issues)
            print(f"An error occurred: {e}. Skipping this item.")

# Concatenating all tables into one
dd = pd.DataFrame()
for item in data:
    dd = pd.concat([dd,item])

# Renaming some items to the needed KWs
dd['Typ'] = dd['Typ'].replace('Blitzangebot','Sonder NL')
dd['Typ'] = dd['Typ'].replace('Thema_NL','Thema')
dd['Typ'] = dd['Typ'].replace('Angebot_NL','Angebot')
dd['Typ'] = dd['Typ'].replace('Unsere_Besten','Unsere Besten')
dd['Typ'] = dd['Typ'].replace('Rezeptheft_NL','Rezeptheft')
dd['Typ'] = dd['Typ'].replace('Rezept_NL','Rezept')

# Writing each NL Analytics to a separate file
folder = '/Volumes/Bilddaten/Grafik_allgemein/E-Commerce/Dominik/'
dd[dd['ListId']=='7'].to_excel(f'{folder}Stats/AU_NL.xlsx',index=False)
dd[dd['ListId']=='10'].to_excel(f'{folder}Stats/FR_NL.xlsx',index=False)
dd[dd['ListId']=='11'].to_excel(f'{folder}Stats/CH_DE_NL.xlsx',index=False)
dd[dd['ListId']=='12'].to_excel(f'{folder}Stats/CH_FR_NL.xlsx',index=False)
dd[dd['ListId']=='528'].to_excel(f'{folder}Stats/HG_NL.xlsx',index=False)
dd[dd['ListId']=='529'].to_excel(f'{folder}Stats/JG_NL.xlsx',index=False)
dd[dd['ListId']=='526'].to_excel(f'{folder}Stats/RZ_FR_NL.xlsx',index=False)
dd[dd['ListId']=='525'].to_excel(f'{folder}Stats/RZ_CHFR_NL.xlsx',index=False)
dd[dd['ListId']=='524'].to_excel(f'{folder}Stats/RZ_CHDE_NL.xlsx',index=False)
dd[dd['ListId']=='523'].to_excel(f'{folder}Stats/RZ_AU_NL.xlsx',index=False)
dd[dd['ListId']=='522'].to_excel(f'{folder}Stats/RZ_DE_NL.xlsx',index=False)
    