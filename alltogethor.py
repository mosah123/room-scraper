import nest_asyncio
nest_asyncio.apply()  # Apply the patch for nested event loops

# Your existing imports
import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urljoin, urlparse
import asyncio
import aiohttp
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import pandas as pd


# Base URL for the city pages
base_url = "https://wearehomesforstudents.com/student-accommodation/"

# List of cities you want to scrape data from
cities = ["nottingham"]  # Expand this list as needed

# List to store all property links
all_properties = []

# Semaphore to limit concurrent requests
semaphore = asyncio.Semaphore(8)  # Limit concurrency to 10

# Function to scrape property links for a given city
def scrape_city(city):
    city_url = f"{base_url}{city}"
    response = requests.get(city_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    property_cards = soup.find_all('a', class_='PropertyCard__link')

    if not property_cards:
        print(f"No property links found for city {city}")
    
    for card in property_cards:
        property_url = card.get('href')
        full_property_url = urljoin(base_url, property_url)
        all_properties.append({
            'City': city,
            'Property URL': full_property_url
        })

# Scrape room links from a property page and ensure they belong to the correct property
def scrape_property_room_links(property_url):
    room_links = []
    try:
        response = requests.get(property_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        room_cards = soup.find_all(class_='RoomCard')

        # Ensure that the room links are from the same property URL
        for room_card in room_cards:
            room_link = urljoin(property_url, room_card.find('a')['href'])
            # Check that the room link starts with the property URL (ensures it's for the correct property)
            if property_url in room_link:
                room_links.append(room_link)
            else:
                print(f"Skipped mismatched room link: {room_link}")
    except Exception as e:
        print(f"Error scraping property rooms: {e}")
    return room_links

# Function to scrape EvoStudent rooms
def scrape_evo_student_property(property_url):
    room_data = []
    try:
        response = requests.get(property_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        # Find all room containers
        room_containers = soup.select('.et_pb_column')
        for room_container in room_containers:
            # Extract room name
            room_name_tag = room_container.select_one('.room-name h2')
            room_name = room_name_tag.get_text(strip=True) if room_name_tag else None
            # Extract price per week and clean it
            price_per_week_tag = room_container.select_one('.price-per-week')
            price_per_week = price_per_week_tag.get_text(strip=True) if price_per_week_tag else None
            if price_per_week:
                price_per_week = clean_room_price(price_per_week)  # Clean the price

            # Only add if 51 weeks tenancy is present
            tenancy_length_tag = room_container.select_one('.tenancy-length')
            tenancy_length = tenancy_length_tag.get_text(strip=True) if tenancy_length_tag else None
            if tenancy_length and '51' in tenancy_length and room_name and price_per_week:
                room_data.append({
                    'City': 'nottingham',
                    'Property URL': property_url,
                    'Room Link': None,  # EvoStudent has no room link
                    'Room Name': room_name,
                    'Room Price': price_per_week,
                    'Date': datetime.now().strftime('%Y-%m-%d')  # Add current date
                })
                print(f"Success - Room Name: {room_name}, Price per Week: {price_per_week}")
            else:
                print(f"Skipping room: {property_url} - Does not meet criteria")
    except Exception as e:
        print(f"Error scraping EvoStudent rooms: {e}")
    return room_data

# Function to clean and convert room price from text to a numeric value
def clean_room_price(price):
    numeric_price = re.sub(r'[^\d.]+', '', price)  # Remove all non-numeric characters
    return float(numeric_price) if numeric_price else None

# Asynchronous function to scrape room prices with increased retries and concurrency limits
async def scrape_room_price(session, room_link, semaphore, retries=5):  # Increased retries to 5
    async with semaphore:
        for attempt in range(retries):
            try:
                print(f"Attempting to scrape: {room_link} (Attempt {attempt + 1}/{retries})")
                async with session.get(room_link) as response:
                    response.raise_for_status()
                    text = await response.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    room_price_tag = soup.find(class_='RoomHero__price')
                    room_price_text = room_price_tag.text.strip() if room_price_tag else 'N/A'
                    room_price = clean_room_price(room_price_text)
                    print(f"Success - Link: {room_link}, Price: {room_price}")
                    return room_link, room_price
            except Exception as e:
                print(f"Failed to retrieve price from {room_link}: {e}")
        print(f"Failed to retrieve price from {room_link} after {retries} attempts.")
        return room_link, 'N/A'

# Function to update or add new prices and their dates in the existing JSON data
def update_room_data(existing_data, new_data):
    for new_room in new_data:
        # Check if it's an EvoStudent property
        if 'evostudent' in new_room['Property URL']:
            # Look for an existing entry for the same room name in EvoStudent property
            matching_room = next((room for room in existing_data if room['Room Name'] == new_room['Room Name'] and room['Property URL'] == new_room['Property URL']), None)

            if matching_room:
                # Check if the price is already present for the same date
                price_keys = [key for key in matching_room.keys() if key.startswith('Room Price')]
                date_keys = [key for key in matching_room.keys() if key.startswith('Date')]

                # Commented out this part to avoid daily price limit during testing
                # last_price_key = f'Room Price{len(price_keys)}'
                # last_date_key = f'Date{len(date_keys)}'

                # if (last_price_key not in matching_room or last_date_key not in matching_room or
                #     matching_room[last_price_key] != new_room['Room Price'] or matching_room[last_date_key] != new_room['Date']):
                #     # Find the next available Room Price and Date keys
                next_price_key = f'Room Price{len(price_keys) + 1}'
                next_date_key = f'Date{len(price_keys) + 1}'
                matching_room[next_price_key] = new_room['Room Price']
                matching_room[next_date_key] = new_room['Date']
            else:
                existing_data.append(new_room)  # If no match, add as a new entry
        else:
            # Check if the room already exists based on Room Link for non-EvoStudent properties
            matching_room = next((room for room in existing_data if room['Room Link'] == new_room['Room Link']), None)
            
            if matching_room:
                # Check if the price is already present for the same date
                price_keys = [key for key in matching_room.keys() if key.startswith('Room Price')]
                date_keys = [key for key in matching_room.keys() if key.startswith('Date')]

                # Commented out this part to avoid daily price limit during testing
                # last_price_key = f'Room Price{len(price_keys)}'
                # last_date_key = f'Date{len(date_keys)}'

                # if (last_price_key not in matching_room or last_date_key not in matching_room or
                #     matching_room[last_price_key] != new_room['Room Price'] or matching_room[last_date_key] != new_room['Date']):
                #     # Find the next available Room Price and Date keys
                next_price_key = f'Room Price{len(price_keys) + 1}'
                next_date_key = f'Date{len(price_keys) + 1}'
                matching_room[next_price_key] = new_room['Room Price']
                matching_room[next_date_key] = new_room['Date']
            else:
                existing_data.append(new_room)
    return existing_data

# Function to convert JSON data to a table format (DataFrame) and save as CSV
def save_json_as_table(json_data, csv_file_path):
    # Convert JSON to pandas DataFrame
    df = pd.json_normalize(json_data)
    # Auto-adjust column widths based on content
    df.to_csv(csv_file_path, index=False)
    print(f"Data saved as table to {csv_file_path}")

# Main function to scrape properties, rooms, and prices
async def scrape_all():
    all_data = []
    start_time = time.time()  # Record start time

    # Load existing JSON data, if available
    json_file_path = 'urbium.json'  # Updated to save in nottingham_properties11.json
    if os.path.exists(json_file_path):
        with open(json_file_path, 'r') as json_file:
            all_data = json.load(json_file)

    new_data = []  # Store new data to update

    # Scrape property links for each city
    with ThreadPoolExecutor() as executor:
        executor.map(scrape_city, cities)

    tasks = []  # List to queue all scraping tasks
    async with aiohttp.ClientSession() as session:
        for property_info in all_properties:
            property_link = property_info['Property URL']
            
            # If the property is an EvoStudent property (no room URLs)
            if 'evostudent' in property_link:
                print(f"Scraping EvoStudent property: {property_link}")
                room_data = scrape_evo_student_property(property_link)
                new_data.extend(room_data)
            else:
                room_links = scrape_property_room_links(property_link)
                
                # Queue all tasks first
                tasks.extend([scrape_room_price(session, room_link, semaphore) for room_link in room_links])

        # Run all tasks concurrently
        prices = await asyncio.gather(*tasks)
        
        for room_link, price in prices:
            room_name = os.path.basename(urlparse(room_link).path)
            property_link = next(
                (prop["Property URL"] for prop in all_properties if prop["Property URL"] in room_link), None
            )
            if property_link:
                new_data.append({
                    'City': property_info['City'],
                    'Property URL': property_link,
                    'Room Link': room_link,
                    'Room Name': room_name,
                    'Room Price': price,
                    'Date': datetime.now().strftime('%Y-%m-%d')  # Add current date
                })

    # Update existing data with new data
    all_data = update_room_data(all_data, new_data)

    # Save the updated data to the JSON file
    with open(json_file_path, 'w') as json_file:
        json.dump(all_data, json_file, indent=4)

    # Convert the JSON data to a table and save it as a CSV
    csv_file_path = 'urbium.csv'
    save_json_as_table(all_data, csv_file_path)

    end_time = time.time()  # Record end time
    elapsed_time = end_time - start_time
    print(f"Scraped {len(new_data)} rooms in {elapsed_time:.2f} seconds and updated JSON file.")

# Run the event loop to execute the async function
if __name__ == "__main__":
    asyncio.run(scrape_all())
