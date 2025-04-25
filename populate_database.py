import sqlite3
import pandas as pd

sheet0 = pd.read_csv('data/shipping_data_0.csv')
sheet1 = pd.read_csv('data/shipping_data_1.csv')
sheet2 = pd.read_csv('data/shipping_data_2.csv')

conn = sqlite3.connect('shipments.db')
cur = conn.cursor()

cur.execute('''
CREATE TABLE IF NOT EXISTS shipments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product TEXT,
    quantity INTEGER,
    shipping_id TEXT,
    origin TEXT,
    destination TEXT,
    driver_id TEXT
);
''')

for _, row in sheet0.iterrows():
    cur.execute('''
        INSERT INTO shipments (product, quantity, shipping_id, origin, destination, driver_id)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        row['product'],
        row['product_quantity'],
        row['driver_identifier'],
        row['origin_warehouse'],
        row['destination_store'],
        row['driver_identifier']
    ))
merged = pd.merge(sheet1, sheet2, left_on='shipment_identifier', right_on='shipment_identifier')

grouped = merged.groupby(['shipment_identifier', 'product', 'origin_warehouse', 'destination_store', 'driver_identifier']).size().reset_index(name='quantity')

for _, row in grouped.iterrows():
    cur.execute('''
        INSERT INTO shipments (product, quantity, shipping_id, origin, destination, driver_id)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        row['product'],
        row['quantity'],
        row['shipment_identifier'],
        row['origin_warehouse'],
        row['destination_store'],
        row['driver_identifier']
    ))
sconn.commit()
conn.close()
