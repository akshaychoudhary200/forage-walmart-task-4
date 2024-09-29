import csv
import sqlite3

# File paths for the uploaded spreadsheets
spreadsheet_0 = '/mnt/data/shipping_data_0.csv'
spreadsheet_1 = '/mnt/data/shipping_data_1.csv'
spreadsheet_2 = '/mnt/data/shipping_data_2.csv'

# Connect to the SQLite database
conn = sqlite3.connect('shipment_database.db')
cursor = conn.cursor()

# Read Spreadsheet 0 and insert products into the product table
# Keep track of product quantities in a dictionary
product_quantity_map = {}
with open('./data/shipping_data_0.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    product_id = 1  # Start an incremental ID for products
    for row in reader:
        product_name = row['product']
        quantity = int(row['product_quantity'])
        product_quantity_map[product_name] = quantity

        # Insert into product table
        cursor.execute('''
            INSERT OR IGNORE INTO main.product (name)
            VALUES (?);
        ''', (product_name,))
        product_id += 1

# Read Spreadsheet 2 (contains origin and destination) and create a map of shipment details
shipment_map = {}
with open('./data/shipping_data_2.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        shipment_id = row['shipment_identifier']
        origin = row['origin_warehouse']
        destination = row['destination_store']
        shipment_map[shipment_id] = (origin, destination)

# Read Shipping_data_1 (contains shipment product data)
with open('./data/shipping_data_1.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        shipment_id = row['shipment_identifier']
        product_name = row['product']

        # Find the product ID from the product table
        cursor.execute('SELECT id FROM main.product WHERE name = ?', (product_name,))
        result = cursor.fetchone()
        if result:
            product_id = result[0]

            # Get the quantity from product_quantity_map
            quantity = product_quantity_map.get(product_name, 0)

            # Get origin and destination from the shipment_map shipping_data_2
            origin, destination = shipment_map[shipment_id]

            # Insert shipment data into the shipment table
            cursor.execute('''
                INSERT INTO main.shipment (id, product_id, quantity, origin, destination)
                VALUES (?, ?, ?, ?, ?);
            ''', (shipment_id, product_id, quantity, origin, destination))

conn.commit()
conn.close()


