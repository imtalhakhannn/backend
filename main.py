from flask import Flask, jsonify
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import concurrent.futures

from flask_cors import CORS
app = Flask(__name__)


CORS(app)

# Extended mappings to include models
MAKE_TO_URL_PARAM = {
    'Audi': {'param': '0.744', 
    'models': 
    {
        '100': '1.744.833',
        '80':'1.744.837',
        '90':'1.744.838',
        'A1':'1.744.2000166',
        'A2':'1.744.6720',
        'A3':'1.744.2046',
        'A4':'1.744.839',
        'A4 allroad':'1.744.2000140',
        'A5':'1.744.8314',
        'A6':'1.744.840',
        'A6 allroad':'1.744.6736',
        'A7':'1.744.2000174',
        'A8':'1.744.841',
        'Q2':'1.744.2000400',
        'Q3':'1.744.2000190',
        'Q4 e-tron':'1.744.2000541',
        'Q5':'1.744.2000097',
        'Q7':'1.744.8149',
        'Q8':'1.744.2000455',
        'Q8 e-tron':'1.744.2000617',
        'R8':'1.744.8340',
        'RS Q8':'1.744.8344',
        'RS3':'1.744.2000184',
        'RS4':'1.744.6721',
        'RS5':'1.744.2000185',
        'RS6':'1.744.7709',
        'RS7':'1.744.2000291',
        'S1':'1.744.2000304',
        'S2':'1.744.7557',
        'S3':'1.744.3731',
        'S4':'1.744.2891',
        'S5':'1.744.2000082',
        'S6':'1.744.843',
        'S7':'1.744.2000246',
        'S8':'1.744.3786',
        'SQ5':'1.744.2000296',
        'SQ7':'1.744.2000402',
        'SQ8':'1.744.8343',
        'TT':'1.744.3732',
        'e-tron':'1.744.2000503',
        'e-tron GT':'1.744.2000540',
        'e-tron GT RS':'1.744.2000539',
        'e-tron Sportback':'1.744.8373',
        'Other':'1.744.2053'
     
     }},
  
    'BMW': {
        'param': '0.749',
        'models': {
            '1-serie': '1.749.7967',
            '1602': '1.749.865',
            '1M': '1.749.2000204',
            '2-serie': '1.749.2000283',
            '2002': '1.749.868',
            '3-serie': '1.749.2132',
            '3-serie GT': '1.749.2000529',
            '4-serie': '1.749.2000265',
            '5-serie': '1.749.2131',
            '5-serie GT': '1.749.2000530',
            '6-serie': '1.749.3004',
            '7-serie': '1.749.2130',
            '8-serie': '1.749.2129',
            'M2': '1.749.2000370',
            'M3': '1.749.893',
            'M4': '1.749.2000295',
            'M5': '1.749.2133',
            'M6': '1.749.8288',
            'M8': '1.749.8362',
            'X1': '1.749.2000133',
            'X2': '1.749.2000440',
            'X3': '1.749.7798',
            'X3 M': '1.749.8358',
            'X4': '1.749.2000294',
            'X4 M': '1.749.8359',
            'X5': '1.749.6737',
            'X5 M': '1.749.8360',
            'X6': '1.749.2000085',
            'X6 M': '1.749.8361',
            'X7': '1.749.2000513',
            'XM': '1.749.2000614',
            'Z1': '1.749.2000034',
            'Z3': '1.749.3003',
            'Z4': '1.749.7683',
            'i3': '1.749.2000264',
            'i4': '1.749.8308',
            'i4 M50': '1.749.2000557',
            'i5': '1.749.2000633',
            'i5 M60': '1.749.2000631',
            'i7': '1.749.2000595',
            'i8': '1.749.2000309',
            'iX M60': '1.749.2000566',
            'iX Drive 40': '1.749.8296',
            'iX Drive 50': '1.749.8297',
            'iX1': '1.749.2000594',
            'iX2': '1.749.1068',
            'iX3': '1.749.2000532',
            'iX40': '1.749.8309',
            'Andre': '1.749.2058'
        }
    },
     'Citroën': {
        'param': '0.757',
        'models': {
            '2CV': '1.757.937',
            'Berlingo': '1.757.3255',
            'Berlingo Electrique': '1.757.7768',
            'C-Crosser': '1.757.8339',
            'C-Zero': '1.757.2000187',
            'C1': '1.757.8237',
            'C2': '1.757.7793',
            'C3': '1.757.7521',
            'C3 Aircross': '1.757.2000446',
            'C3 Picasso': '1.757.2000116',
            'C4': '1.757.7986',
            'C4 Aircross': '1.757.2000211',
            'C4 Cactus': '1.757.2000292',
            'C4 Picasso': '1.757.1492',
            'C5': '1.757.7115',
            'C5 Aircross': '1.757.2000514',
            'C5 X': '1.757.2000604',
            'C6': '1.757.8203',
            'C8': '1.757.7660',
            'CX': '1.757.943',
            'DS3': '1.757.2000154',
            'DS4': '1.757.2000195',
            'DS5': '1.757.2000207',
            'E-C4': '1.757.2000544',
            'E-C4 X': '1.757.2000615',
            'Grand C4 Picasso': '1.757.2000080',
            'Grand C4 Spacetourer': '1.757.2000449',
            'Jumper': '1.757.950',
            'Jumpy': '1.757.7517',
            'Space Tourer': '1.757.2000434',
            'XM': '1.757.954',
            'Xantia': '1.757.953',
            'Xsara Picasso': '1.757.7861',
            'ZX': '1.757.955',
            'Andre': '1.757.2066'
        }
    },
   
}

    


TRANSMISSION_TO_URL_PARAM = {
    'Manual': '1',
    'Automatic': '2'
}

SALES_FORM_TO_URL_PARAM = {
    # 'New Car': '1',
    # 'Used Car': '2'
}

# def create_connection():
#     try:
#         conn = mysql.connector.connect(
#             host='localhost',
#             user='root',
#             password='imTalha18',
#             database='carsautomation'
#         )
#         return conn
#     except Error as e:
#         print(f"Database connection failed: {e}")
#         return None
def create_connection():
    try:
        conn = mysql.connector.connect(
            host='13.41.199.227',
            user='root',
            password='root123',
            port=3309,
            database='carsautomation'
        )
        return conn
    except Error as e:
        print(f"Database connection failed: {e}")
        return None

def update_listing_status():
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE listings SET status = 'sold' WHERE scrap_date < CURDATE() AND status = 'active'")
            conn.commit()
        except Error as e:
            print(f"Failed to update listing status: {e}")
        finally:
            cursor.close()
            conn.close()

def log_history(cursor, listing_id):
    try:
        cursor.execute("""
            INSERT INTO listing_history (listing_id, make, model, year, mileage, price, included_in, scrap_date, status)
            SELECT id, make, model, year, mileage, price, included_in, scrap_date, status FROM listings WHERE id = %s
        """, (listing_id,))
    except Error as e:
        print(f"Failed to log history for listing {listing_id}: {e}")

def insert_car_data(data):
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        try:
            for item in data:
                cursor.execute("SELECT id, price FROM listings WHERE make = %s AND model = %s AND mileage = %s", 
                               (item['make'], item['model'], item['mileage']))
                result = cursor.fetchone()
                if result:
                    if result[1] != item['price']:
                        log_history(cursor, result[0])
                    cursor.execute("""
                        UPDATE listings SET year=%s, price=%s, included_in=%s, scrap_date=%s, status='active' WHERE id=%s
                    """, (item['year'], item['price'], item['included_in'], item['scrap_date'], result[0]))
                else:
                    cursor.execute("""
                        INSERT INTO listings (make, model, year, mileage, price, included_in, scrap_date, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, 'active')
                    """, (item['make'], item['model'], item['year'], item['mileage'], item['price'], item['included_in'], item['scrap_date']))
                    new_id = cursor.lastrowid
                    log_history(cursor, new_id)
            conn.commit()
        except Error as e:
            print(f"Failed to insert/update car data: {e}")
        finally:
            cursor.close()
            conn.close()
import re

import unidecode

def normalize_text(text):
    """Convert text to lower case and remove accents."""
    return unidecode.unidecode(text.lower())

def parse_make_model_from_name(name):
    """Extract make and model from the car name using regular expressions for improved flexibility,
    automatically including any variants found in the models dictionary."""
    normalized_name = normalize_text(name)  # Normalize the name to lowercase and remove accents for better matching
    for make, data in MAKE_TO_URL_PARAM.items():
        make_pattern = re.escape(normalize_text(make))  # Safely escape the make name for regex use and normalize
        if re.search(make_pattern, normalized_name):  # Use regex search to find the make
            for model, model_param in data['models'].items():
                model_pattern = re.escape(normalize_text(model))  # Normalize and escape model name
                if re.search(model_pattern, normalized_name):
                    return make, model  # Return the make and the exact model match
    return "Unknown", "Unknown"

def scrape_data(url, metadata):
    car_listings = []
    page_number = 1
    article_class = "sf-search-ad-legendary"
    grid_div_class = "grid-flow-row-dense"
    while True:
        response = requests.get(url, params={'page': page_number})
        if response.status_code != 200:
            break
        soup = BeautifulSoup(response.text, 'html.parser')
        car_grid_div = soup.find('div', class_=grid_div_class)
        car_articles = car_grid_div.find_all('article', class_=article_class) if car_grid_div else []
        if not car_articles:
            break
        car_listings.extend(extract_car_data(car_articles, metadata))
        page_number += 1
    return car_listings
def extract_car_data(articles, metadata):
    car_listings = []
    scrap_date = datetime.now().strftime("%Y-%m-%d")
    for article in articles:
        car_name_tag = article.find('h2', class_='break-words')
        car_name = car_name_tag.text.strip() if car_name_tag else 'No name found'
        make, model = parse_make_model_from_name(car_name)  # Parse make and model from the car name
        data_div_class = "justify-between"
        data_div = article.find('div', class_=data_div_class)
        spans = data_div.find_all('span') if data_div else []
        details = [span.text.strip() for span in spans]

        # Determine transmission from car name or default to metadata if not explicitly mentioned
        transmission = 'Unknown'  # Default value
        if 'manual' in car_name.lower():
            transmission = 'Manual'
        elif 'automatic' in car_name.lower():
            transmission = 'Automatic'
        else:
            transmission = metadata.get('transmission', 'Unknown')  # Use metadata or default to 'Unknown'

        # Included_in only contains the transmission type
        included_in_value = transmission

        listing_info = {
            'make': make,
            'model': model,
            'name': car_name,
            'year': details[0] if details else 'Unknown',
            'mileage': details[1] if len(details) > 1 else 'Unknown',
            'price': details[2] if len(details) > 2 else 'Unknown',
            'scrap_date': scrap_date,
            'included_in': included_in_value
        }
        print(listing_info)
        car_listings.append(listing_info)
    return car_listings

@app.route('/scrape_all', methods=['GET'])
def scrape_all():
    update_listing_status()
    all_car_listings = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {}
        # Create tasks for scraping data for each transmission and model
        for make, data in MAKE_TO_URL_PARAM.items():
            for model, model_param in data['models'].items():
                for transmission, transmission_param in TRANSMISSION_TO_URL_PARAM.items():
                    url = f'https://www.finn.no/car/used/search.html?model={model_param}&transmission={transmission_param}'
                    metadata = {'make': make, 'model': model, 'transmission': transmission}
                    future_to_url[executor.submit(scrape_data, url, metadata)] = url
        # Collect results from futures
        for future in concurrent.futures.as_completed(future_to_url):
            all_car_listings.extend(future.result())
    insert_car_data(all_car_listings)
    return jsonify(all_car_listings)

from flask import Flask, request, jsonify
def clean_price(price):
    if price == "Solgt":
        return 0  # Return 0 if the price is 'Solgt'
    # Remove ' kr' and non-breaking spaces, and then remove any remaining spaces
    price = price.replace(' kr', '').replace('\xa0', '').replace(' ', '')
    return int(price) if price.isdigit() else 0

@app.route('/api/prices', methods=['GET'])
def get_prices():
    make = request.args.get('make', type=str)
    model = request.args.get('model', type=str)
    transmission = request.args.get('transmission', type=str)
    year = request.args.get('year', default=None, type=str)
    
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        query = """
        SELECT year, REPLACE(REPLACE(REPLACE(price, ' kr', ''), '\\xa0', ''), ' ', '') AS original_price
        FROM listings
        WHERE make = %s AND model = %s AND included_in = %s {}
        ORDER BY year;
        """
        year_condition = "AND year = %s" if year else ""
        query = query.format(year_condition)
        params = (make, model, transmission, year) if year else (make, model, transmission)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Ensure prices are cleaned and converted to integers
        prices_data = [{'year': row[0], 'original_price': int(clean_price(row[1]))} for row in results]
        
        return jsonify(prices_data)
    else:
        return jsonify({"error": "Database connection failed"}), 500
    

@app.route('/api/models', methods=['GET'])
def get_models():
    make = request.args.get('make', type=str)
    
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        query = """
        SELECT DISTINCT model
        FROM listings
        WHERE make = %s;
        """
        cursor.execute(query, (make,))
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Extracting model names from the results and returning them
        models_list = [{'model': row[0]} for row in results]
        return jsonify(models_list)
    else:
        return jsonify({"error": "Database connection failed"}), 500



@app.route('/api/years', methods=['GET'])
def get_years():
    make = request.args.get('make', type=str)
    model = request.args.get('model', type=str)

    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        query = """
        SELECT DISTINCT year
        FROM listings
        WHERE make = %s AND model = %s
        ORDER BY year;
        """
        cursor.execute(query, (make, model))
        results = cursor.fetchall()
        cursor.close()
        conn.close()

        # Create a list of years from the query results.
        years = [{'year': row[0]} for row in results]
        return jsonify(years)
    else:
        return jsonify({"error": "Database connection failed"}), 500

from statistics import median
# def log_median_price(make, model, transmission_type, median_price):
#     """Function to log the calculated median price to the database, avoiding duplicates and skipping zeros."""
#     conn = create_connection()
#     if conn:
#         cursor = conn.cursor()
#         try:
#             if median_price > 0:  # Check if the median price is greater than 0
#                 # First, check if this exact log entry already exists for today
#                 check_query = """
#                 SELECT 1 FROM median_price_log
#                 WHERE make = %s AND model = %s AND transmission_type = %s AND median_price = %s AND log_date = CURDATE()
#                 """
#                 cursor.execute(check_query, (make, model, transmission_type, median_price))
#                 exists = cursor.fetchone()

#                 if not exists:
#                     # Only insert if no existing log for this entry on the same day
#                     insert_query = """
#                     INSERT INTO median_price_log (make, model, transmission_type, median_price, log_date)
#                     VALUES (%s, %s, %s, %s, CURDATE())
#                     """
#                     cursor.execute(insert_query, (make, model, transmission_type, median_price))
#                     conn.commit()
#                 else:
#                     print("Log entry for today already exists. Skipping insert.")
#             else:
#                 print("Median price is 0. Skipping log entry.")
#         except Error as e:
#             print(f"Failed to log median price: {e}")
#         finally:
#             cursor.close()
#             conn.close()


# @app.route('/api/median_prices_his', methods=['GET'])
# def get_pricdfdes():
#     """API endpoint to calculate and return median prices for given car specifications."""
#     make = request.args.get('make')
#     model = request.args.get('model')
#     transmission_type = request.args.get('transmission_type')

#     conn = create_connection()
#     if conn:
#         cursor = conn.cursor()
#         query = """
#         SELECT REPLACE(REPLACE(REPLACE(price, ' kr', ''), '\\xa0', ''), ' ', '') AS original_price
#         FROM listings
#         WHERE make = %s AND model = %s AND included_in = %s
#         """
#         params = (make, model, transmission_type)
#         cursor.execute(query, params)
#         results = cursor.fetchall()
#         cursor.close()
#         conn.close()

#         prices = [clean_price(row[0]) for row in results if clean_price(row[0]) > 0]
#         median_price = median(prices) if prices else 0
        
#         log_median_price(make, model, transmission_type, median_price)  # Log the median price

#         return jsonify({'make': make, 'model': model, 'transmission_type': transmission_type, 'median_price': median_price})
#     else:
#         return jsonify({"error": "Database connection failed"}), 500
def log_median_price(make, model, included_in, median_price):
    """Function to log the calculated median price to the database, avoiding duplicates and skipping zeros."""
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        try:
            if median_price > 0:
                conn.autocommit = False  # Disable autocommit to manage transaction manually
                # Set the transaction isolation level to serializable
                cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

                check_query = """
                SELECT 1 FROM median_price_log
                WHERE make = %s AND model = %s AND transmission_type = %s AND median_price = %s AND log_date = CURDATE()
                """
                cursor.execute(check_query, (make, model, included_in, median_price))
                exists = cursor.fetchone()

                if not exists:
                    insert_query = """
                    INSERT INTO median_price_log (make, model, transmission_type, median_price, log_date)
                    VALUES (%s, %s, %s, %s, CURDATE())
                    """
                    cursor.execute(insert_query, (make, model, included_in, median_price))
                    conn.commit()  # Commit the transaction
                    print("Log entry for median price added.")
                else:
                    print("Log entry for today already exists. Skipping insert.")
                conn.autocommit = True  # Re-enable autocommit
            else:
                print("Median price is 0. Skipping log entry.")
        except Exception as e:
            conn.rollback()  # Roll back in case of error
            print(f"Failed to log median price: {e}")
        finally:
            cursor.close()
            conn.close()

def get_unique_combinations():
    """Retrieve all unique combinations of make, model, and included_in from the database for today's date."""
    today_date = datetime.today().strftime('%Y-%m-%d')  # Format today's date as YYYY-MM-DD
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        try:
            query = """
            SELECT DISTINCT make, model, included_in FROM listings
            WHERE scrap_date = %s
            """
            cursor.execute(query, (today_date,))
            combinations = cursor.fetchall()
            return combinations
        finally:
            cursor.close()
            conn.close()
    return []

def calculate_and_log_median_prices():
    """Calculate and log median prices for all unique combinations from today's date."""
    today_date = datetime.today().strftime('%Y-%m-%d')
    combinations = get_unique_combinations()
    for make, model, included_in in combinations:
        conn = create_connection()
        if conn:
            cursor = conn.cursor()
            query = """
            SELECT REPLACE(REPLACE(REPLACE(price, ' kr', ''), '\\xa0', ''), ' ', '') AS original_price
            FROM listings
            WHERE make = %s AND model = %s AND included_in = %s AND scrap_date = %s
            """
            params = (make, model, included_in, today_date)
            cursor.execute(query, params)
            results = cursor.fetchall()
            cursor.close()
            conn.close()

            prices = [clean_price(row[0]) for row in results if clean_price(row[0]) > 0]
            median_price = median(prices) if prices else 0
            
            log_median_price(make, model, included_in, median_price)  # Log the median price

@app.route('/api/median_prices_his', methods=['GET'])
def get_median_prices():
    """API endpoint to calculate and log median prices for all car combinations based on today's scrap date."""
    try:
        calculate_and_log_median_prices()
        return jsonify({"status": "Success", "message": "Median prices calculated and logged for today."})
    except Exception as e:
        return jsonify({"status": "Error", "message": str(e)}), 500














@app.route('/api/years_graph', methods=['GET'])
def get_yearsss():
    make = request.args.get('make', type=str)
    model = request.args.get('model', type=str)
    transmission = request.args.get('transmission', default=None, type=str)

    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        query = """
        SELECT DISTINCT year
        FROM listings
        WHERE make = %s AND model = %s
        """
        params = [make, model]

        # Adjusting the query to use the included_in column for transmission
        if transmission:
            query += " AND included_in LIKE %s"
            params.append('%' + transmission + '%')

        query += " ORDER BY year;"
        cursor.execute(query, tuple(params))
        results = cursor.fetchall()
        cursor.close()
        conn.close()

        # Create a list of years from the query results.
        years = [{'year': row[0]} for row in results]
        return jsonify(years)
    else:
        return jsonify({"error": "Database connection failed"}), 500
    
def get_unique_combinations_yearly():
    """Retrieve all unique combinations of make, model, year, and transmission_type from the database for today's date."""
    today_date = datetime.today().strftime('%Y-%m-%d')  # Format today's date as YYYY-MM-DD
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        try:
            query = """
            SELECT DISTINCT make, model, year, included_in FROM listings
            WHERE scrap_date = %s
            """
            cursor.execute(query, (today_date,))
            combinations = cursor.fetchall()
            return combinations
        finally:
            cursor.close()
            conn.close()
    return []

def log_median_price_yearly(make, model, year, transmission_type, median_price):
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        try:
            if median_price > 0:
                conn.autocommit = False
                cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

                check_query = """
                SELECT 1 FROM median_price_log_yearly
                WHERE make = %s AND model = %s AND year = %s AND transmission_type = %s AND median_price = %s AND log_date = CURDATE()
                """
                cursor.execute(check_query, (make, model, year, transmission_type, median_price))
                exists = cursor.fetchone()

                if not exists:
                    insert_query = """
                    INSERT INTO median_price_log_yearly (make, model, year, transmission_type, median_price, log_date)
                    VALUES (%s, %s, %s, %s, %s, CURDATE())
                    """
                    cursor.execute(insert_query, (make, model, year, transmission_type, median_price))
                    conn.commit()
                    print("Log entry for median price added.")
                else:
                    print("Log entry for today already exists. Skipping insert.")
                conn.autocommit = True
            else:
                print("Median price is 0. Skipping log entry.")
        except Exception as e:
            conn.rollback()
            print(f"Failed to log median price: {e}")
        finally:
            cursor.close()
            conn.close()



def calculate_and_log_median_prices_yearly():
    """Calculate and log median prices for all unique combinations from today's date."""
    today_date = datetime.today().strftime('%Y-%m-%d')
    combinations = get_unique_combinations_yearly()
    for make, model, year, included_in in combinations:
        conn = create_connection()
        if conn:
            cursor = conn.cursor()
            query = """
            SELECT REPLACE(REPLACE(REPLACE(price, ' kr', ''), '\\xa0', ''), ' ', '') AS original_price
            FROM listings
            WHERE make = %s AND model = %s AND year = %s AND  included_in = %s AND scrap_date = %s
            """
            params = (make, model, year, included_in, today_date)
            cursor.execute(query, params)
            results = cursor.fetchall()
            cursor.close()
            conn.close()

            prices = [clean_price(row[0]) for row in results if clean_price(row[0]) > 0]
            median_price = median(prices) if prices else 0

            log_median_price_yearly(make, model, year, included_in, median_price)

@app.route('/api/median_prices_yearly', methods=['GET'])
def get_median_prices_yearly():
    """API endpoint to calculate and log median prices for all car combinations based on today's scrap date, including the model year."""
    try:
        calculate_and_log_median_prices_yearly()  # This function should be updated as described previously
        return jsonify({"status": "Success", "message": "Median prices calculated and logged for today, including year differentiation."})
    except Exception as e:
        return jsonify({"status": "Error", "message": str(e)}), 500
    


@app.route('/api/median_prices_history_yearly', methods=['GET'])
def median_price_history():
    make = request.args.get('make')
    model = request.args.get('model')
    transmission_type = request.args.get('transmission_type')

    if not all([make, model, transmission_type]):
        return jsonify({"status": "Error", "message": "Missing parameters. Please provide make, model, and transmission_type."}), 400

    data = fetch_median_prices(make, model, transmission_type)
    return jsonify(data)

def fetch_median_prices(make, model, transmission_type):
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        try:
            query = """
            SELECT year, median_price, log_date FROM median_price_log_yearly
            WHERE make = %s AND model = %s AND transmission_type = %s
            ORDER BY year, log_date
            """
            cursor.execute(query, (make, model, transmission_type))
            results = cursor.fetchall()
            prices = [{"year": row[0], "date": row[2].strftime('%Y-%m-%d'), "median_price": f"{row[1]:.2f}"} for row in results]
            return prices
        except Exception as e:
            print(f"Failed to fetch median prices: {e}")
            return []
        finally:
            cursor.close()
            conn.close()
    return []

    

@app.route('/api/median_prices_history_yearly_date', methods=['GET'])
def median_price_history_yearly_date():
    make = request.args.get('make')
    model = request.args.get('model')
    transmission_type = request.args.get('transmission_type')
    year = request.args.get('year')  # Get the year from query parameters

    if not all([make, model, transmission_type]):
        return jsonify({"status": "Error", "message": "Missing parameters. Please provide make, model, and transmission_type."}), 400

    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        try:
            # Modify the query to filter by year if it's provided
            query = """
            SELECT year, median_price, log_date FROM median_price_log_yearly
            WHERE make = %s AND model = %s AND transmission_type = %s
            """
            params = [make, model, transmission_type]

            if year:
                query += " AND year = %s"
                params.append(year)

            query += " ORDER BY year, log_date"
            
            cursor.execute(query, tuple(params))
            results = cursor.fetchall()
            prices = [{"year": row[0], "date": row[2].strftime('%Y-%m-%d'), "median_price": f"{row[1]:.2f}"} for row in results]
            return jsonify(prices)
        except Exception as e:
            print(f"Failed to fetch median prices: {e}")
            return jsonify({"status": "Error", "message": "Failed to fetch data"}), 500
        finally:
            cursor.close()
            conn.close()
    else:
        return jsonify({"status": "Error", "message": "Failed to connect to database"}), 500



@app.route('/api/median_price_history', methods=['GET'])
def get_median_price_history():
    make = request.args.get('make')
    model = request.args.get('model')
    transmission_type = request.args.get('transmission_type')

    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        query = """
        SELECT median_price, log_date
        FROM median_price_log
        WHERE make = %s AND model = %s AND transmission_type = %s
        ORDER BY log_date ASC
        """
        cursor.execute(query, (make, model, transmission_type))
        results = cursor.fetchall()
        cursor.close()
        conn.close()

        history = [{'date': str(row[1]), 'median_price': row[0]} for row in results]
        return jsonify(history)
    else:
        return jsonify({"error": "Database connection failed"}), 500

@app.route('/car_listing', methods=['POST'])
def create_car_listing():
    data = request.get_json()
    make = data['make']
    model = data['model']
    year = data['year']
    mileage = data['mileage']
    price = data['price']
    included_in = data['included_in']

    conn = create_connection()
    cursor = conn.cursor()
    query = """
    INSERT INTO listings(make, model, year, mileage, price, included_in, scrap_date, status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (make, model, year, mileage, price, included_in, datetime.utcnow(), 'active')
    cursor.execute(query, values)
    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({'message': 'Car listing created successfully'}), 201

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0')
