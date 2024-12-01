import pandas as pd
import requests
import os
import time
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm, trange
from logzero import logger


def authenticate(email, password):
    auth_url = 'https://api.jinka.fr/apiv2/user/auth'
    auth_dict = {'email': email, 'password': password}
    s = requests.Session()
    r_auth = s.post(auth_url, auth_dict)
    if r_auth.status_code == 200:
        logger.info('Authentification succeeded (200)')
        access_token = r_auth.json()['access_token']
    else:
        logger.critical(f'Authentification failed with error {r_auth.status_code}')
        return None, None

    headers = {
        'Accept': '*/*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
        'Accept-Language': 'fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}',
        'Origin': 'https://www.jinka.fr',
        'Connection': 'keep-alive',
        'DNT': '1',
        'Sec-GPC': '1',
        'If-None-Match': 'W/f46-qWZd5Nq9sjWAv9cj3oEhFaxFuek',
        'TE': 'Trailers',
    }

    return s, headers


def get_alerts(session, headers):
    logger.info("Fetching alerts from Jinka API.")
    r_alerts = session.get('https://api.jinka.fr/apiv2/alert', headers=headers)

    # Vérification du statut de la requête
    if r_alerts.status_code != 200:
        logger.error(f"Failed to fetch alerts. Status code: {r_alerts.status_code}")
        logger.error(f"Response: {r_alerts.text}")
        return pd.DataFrame()

    try:
        alerts_data = r_alerts.json()
        logger.info(f"Fetched {len(alerts_data)} alerts.")
    except Exception as e:
        logger.error(f"Failed to parse JSON response: {e}")
        logger.error(f"Response: {r_alerts.text}")
        return pd.DataFrame()

    # Extraction des données
    df_alerts = pd.DataFrame(columns=['id', 'name', 'user_name', 'ads_per_day'])
    data_dict = {
        'id': [], 'name': [], 'user_name': [], 'ads_per_day': [],
        'nb_pages': [], 'all': [], 'read': [], 'unread': [],
        'favorite': [], 'contact': [], 'deleted': []
    }

    for counter, alert in enumerate(alerts_data):
        data_dict['id'].append(alert.get('id'))
        data_dict['name'].append(alert.get('name'))
        data_dict['user_name'].append(alert.get('user_name'))
        data_dict['ads_per_day'].append(alert.get('estimated_ads_per_day'))

        root_url = f"https://api.jinka.fr/apiv2/alert/{alert.get('id')}/dashboard"
        r_pagination = session.get(root_url, headers=headers)
        if r_pagination.status_code == 200:
            pagination_data = r_pagination.json().get('pagination', {})
            data_dict['nb_pages'].append(pagination_data.get('nbPages', 0))
            totals = pagination_data.get('totals', {})
            data_dict['all'].append(totals.get('all', 0))
            data_dict['read'].append(totals.get('read', 0))
            data_dict['unread'].append(totals.get('unread', 0))
            data_dict['favorite'].append(totals.get('favorite', 0))
            data_dict['contact'].append(totals.get('contact', 0))
            data_dict['deleted'].append(totals.get('deleted', 0))
        else:
            logger.warning(f"Failed to fetch pagination data for alert ID {alert.get('id')}.")

        logger.info(f"{counter + 1} / {len(alerts_data)} alerts have been processed.")

    # Convertir les données en DataFrame
    df_alerts = pd.DataFrame(data=data_dict)

    # Affichage des alertes et leurs informations
    for _, alert in df_alerts.iterrows():
        print(f"\nAlert ID: {alert['id']}")
        print(f"Name: {alert['name']}")
        print(f"User: {alert['user_name']}")
        print(f"Estimated Ads per Day: {alert['ads_per_day']}")
        print(f"Number of Pages: {alert['nb_pages']}")
        print(f"Total Ads: {alert['all']}")
        print(f"Read: {alert['read']}")
        print(f"Unread: {alert['unread']}")
        print(f"Favorites: {alert['favorite']}")
        print(f"Contacts: {alert['contact']}")
        print(f"Deleted: {alert['deleted']}")

    return df_alerts


def get_appart_response(session, row_tuple):
    alert_id = row_tuple[1]['alert_id']
    appart_id = str(row_tuple[0])

    headers = {
        'authority': 'api.jinka.fr',
        'upgrade-insecure-requests': '1',
        'dnt': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'same-site',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '?1',
        'sec-fetch-dest': 'document',
        'accept-language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
    }

    params = (('ad', appart_id), ('alert_token', alert_id))
    try:
        response = session.get('https://api.jinka.fr/alert_result_view_ad', headers=headers, params=params)
        response.raise_for_status()  # Vérifier si la requête a réussi
        logger.info(f"Fetched URL for ad ID {appart_id}: {response.url}")

        # Validation stricte pour éviter les liens génériques
        if "jinka.fr" in response.url and "alert_result_view_ad" not in response.url:
            logger.warning(f"Unexpected URL format for ad ID {appart_id}: {response.url}")
            return None

        return response.url  # Retourner l'URL spécifique si valide
    except Exception as e:
        logger.warning(f"Error fetching ad {appart_id}: {e}. Retrying in 30 seconds.")
        time.sleep(30)
        try:
            response = session.get('https://api.jinka.fr/alert_result_view_ad', headers=headers, params=params)
            response.raise_for_status()
            logger.info(f"Retry succeeded for ad ID {appart_id}: {response.url}")

            # Même validation après le retry
            if "jinka.fr" in response.url and "alert_result_view_ad" not in response.url:
                logger.warning(f"Unexpected URL format after retry for ad ID {appart_id}: {response.url}")
                return None

            return response.url
        except Exception as retry_error:
            logger.error(f"Retry failed for ad {appart_id}: {retry_error}")
            return None  # Retourne None si l'erreur persiste


def expired_checker(response, row_tuple):
    source = row_tuple[1]['source']
    true_expired_date = None

    if source in ['logic-immo', 'century21', 'meilleursagents', 'locservice', 'lagenceblue']:
        parsed_url = BeautifulSoup(response.text, 'html.parser')
    elif source in ['pap', 'seloger', 'paruvendu', 'laforet', 'orpi', 'avendrealouer', 'fnaim', 'locatair']:
        parsed_url = response.url.split('/')
    elif source == 'leboncoin':
        true_expired_date = row_tuple[1]['expired_at']
    else:
        return true_expired_date

    if source == 'logic-immo':
        item = parsed_url.find_all(class_="expiredTxt")
        if len(item) != 0:
            true_expired_date = datetime.now()

    if source == 'pap':
        if parsed_url[3] == 'annonce':
            true_expired_date = datetime.now()

    if source == 'seloger':
        if parsed_url[-1] == '#expiree':
            true_expired_date = datetime.now()

    if source == 'explorimmo':
        pass

    if source == 'paruvendu':
        if parsed_url[-1] == '#showError404':
            true_expired_date = datetime.now()

    if source == 'century21':
        item = parsed_url.find_all(class_="content_msg")
        item2 = parsed_url.find_all(class_="tw-font-semibold tw-text-lg")
        if len(item) != 0:
            if item[0].strong.text == "Nous sommes désolés, la page à laquelle vous tentez d'accéder n'existe pas.":
                true_expired_date = datetime.now()
        if len(item2) != 0:
            if item2[
                0].text.strip() == "Cette annonce est désactivée, retrouvez ci-dessous une sélection de biens s'en rapprochant.":
                true_expired_date = datetime.now()

    if source == 'stephaneplaza':
        pass

    if source == 'meilleursagents':
        item = parsed_url.find_all(class_="error-page")
        if len(item) != 0:
            true_expired_date = datetime.now()

    if source == 'flatlooker':
        pass

    if source == 'bienici':
        pass

    if source == 'locservice':
        item = parsed_url.find_all(class_="louerecemment")
        if len(item) != 0:
            true_expired_date = datetime.now()

    if source == 'guyhoquet':
        pass

    if source == 'laforet':
        if parsed_url[3] == 'ville':
            true_expired_date = datetime.now()

    if source == 'lagenceblue':
        item = parsed_url.find_all(class_="label label-warning")
        if len(item) != 0:
            true_expired_date = datetime.now()

    if source == 'avendrealouer':
        if '#expiree' in parsed_url[-1]:
            true_expired_date = datetime.now()

    if source == 'orpi':
        if parsed_url[-2] == 'louer-appartement':
            true_expired_date = datetime.now()

    if source == 'parisattitude':
        pass

    if source == 'fnaim':
        if len(parsed_url) >= 3:
            if parsed_url[3] != 'annonce-immobiliere':
                true_expired_date = datetime.now()

    if source == 'erafrance':
        pass

    return true_expired_date


def get_all_links(session, df, expired, appart_db_path):
    if os.path.exists(appart_db_path) and not expired:
        logger.info('Found a preexisting links database.')
        df['link'] = None
        df_already_processed = pd.read_json(appart_db_path, orient='columns')
        unprocessed_index = set(df.index) - set(df_already_processed.index)
        processed_index = set(df.index).intersection(df_already_processed.index)
        df.loc[list(processed_index), 'link'] = df_already_processed['link']
    else:
        if not os.path.exists(appart_db_path):
            logger.warning('No preexisting database has been found, generating a new one.')
        elif expired:
            logger.warning('Replacing the previous database in order to check for apparts expiration.')
        unprocessed_index = df.index
        df_already_processed = pd.DataFrame()

    links = []  # Initialiser une liste vide pour stocker les liens

    for row_tuple in tqdm(df.iterrows(), total=len(df)):
        if row_tuple[0] not in unprocessed_index:
            continue  # Ne traiter que les indices non déjà traités
        response_url = get_appart_response(session, row_tuple)
        if response_url:
            links.append(response_url)
        else:
            links.append("Invalid link")  # Indiquer explicitement un lien invalide

    # Ajouter les liens au DataFrame
    df.loc[list(unprocessed_index), 'link'] = links  # Conversion de set en liste
    df_to_append = df.loc[list(unprocessed_index), ['link']]  # Conversion de set en liste

    # Utiliser pd.concat à la place de append
    df_already_processed = pd.concat([df_already_processed, df_to_append])
    df_already_processed.to_json(appart_db_path, orient='columns')

    return df

    links = []  # Initialiser une liste vide pour stocker les liens

    for row_tuple in tqdm(df.iterrows(), total=len(df)):
        if row_tuple[0] not in unprocessed_index:
            continue  # Ne traiter que les indices non déjà traités
        response = get_appart_response(session, row_tuple)
        true_url = response.url
        links.append(true_url)  # Ajouter le lien à la liste

    # Ajouter les liens au DataFrame
    df.loc[list(unprocessed_index), 'link'] = links  # Conversion de set en liste
    df_to_append = df.loc[list(unprocessed_index), ['link']]  # Conversion de set en liste

    # Utiliser pd.concat à la place de append
    df_already_processed = pd.concat([df_already_processed, df_to_append])
    df_already_processed.to_json(appart_db_path, orient='columns')

    return df


def remove_expired(session, df, last_deleted_path):
    df_expired = df.loc[df["expired_at"].notna(), :]
    if len(df_expired) > 15:
        logger.critical('Df slicing error')
        exit()
    logger.info('Starting the cleaning of expired offers.')
    for appart_id, row in tqdm(df_expired.iterrows()):
        post_url = 'https://api.jinka.fr/apiv2/alert/' + row['alert_id'] + '/abuses'
        data = {'ad_id': appart_id, 'reason': 'ad_link_404'}
        session.post(post_url, data=data)
    df_expired.to_json(last_deleted_path, orient='columns')
    cleaned_df = df.loc[df['expired_at'].isna(), :]
    logger.info(f'Finished cleaning the {len(df_expired)} expired appartments.')
    return cleaned_df


def get_apparts(session, headers, alert_id, nb_pages):
    root_url = 'https://api.jinka.fr/apiv2/alert/' + str(alert_id) + '/dashboard'
    df_apparts = pd.DataFrame(
        columns=['id', 'source', 'source_is_partner', 'source_logo', 'source_label', 'search_type', 'owner_type', \
                 'rent', 'rent_max', 'area', 'room', 'bedroom', 'floor', 'type', 'buy_type', 'city', 'postal_code',
                 'lat', 'lng', 'furnished', \
                 'description', 'description_is_truncated', 'images', 'created_at', 'expired_at', 'sendDate',
                 'previous_rent', 'previous_rent_at', \
                 'favorite', 'nb_spam', 'contacted', 'stops', 'features', 'new_real_estate', 'rentMinPerM2',
                 'clicked_at', 'webview_link', 'alert_id', \
                 'page'])
    for page in trange(1, nb_pages + 1):
        target_url = root_url + f'?filter=all&page={page}'
        r_apparts = session.get(target_url, headers=headers)
        df_temp = pd.DataFrame.from_records(data=r_apparts.json()['ads'])
        df_temp['page'] = page
        df_apparts = pd.concat([df_apparts, df_temp], ignore_index=True)
    return df_apparts


def get_all_apparts(df_alerts, session, headers):
    df_final = pd.DataFrame(columns=['id', 'source', 'source_is_partner', 'source_logo', 'source_label',
                                     'search_type', 'owner_type', 'rent', 'rent_max', 'area', 'room', 'bedroom',
                                     'floor', 'type', 'buy_type',
                                     'city', 'postal_code', 'lat', 'lng', 'furnished', 'description',
                                     'description_is_truncated', 'images',
                                     'created_at', 'expired_at', 'sendDate', 'previous_rent', 'previous_rent_at',
                                     'favorite', 'nb_spam', 'contacted',
                                     'stops', 'features', 'new_real_estate', 'rentMinPerM2', 'clicked_at',
                                     'webview_link', 'alert_id'])
    for idx, alert in df_alerts.iterrows():
        logger.info(f'Starting the processing of the apparts of alert n°{idx + 1}')
        alert_id = alert['id']
        nb_pages = alert['nb_pages']
        df_alert = get_apparts(session, headers, alert_id, nb_pages)
        df_final = pd.concat([df_final, df_alert], ignore_index=True)
        logger.info(f'Finished processing the apparts of alert n°{idx + 1}')
    df_final = df_final.set_index('id')
    expired_index = df_final[df_final['expired_at'].notna()].index
    logger.warning(f"{len(expired_index)} apparts have expired.")
    return df_final, expired_index