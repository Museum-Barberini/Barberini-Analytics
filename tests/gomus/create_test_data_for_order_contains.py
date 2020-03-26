# link to a specific order:
# https://barberini.gomus.de/admin/orders/ORDER-ID

# Use this to add test cases to scrape_order_contains_data.csv
import mmh3

hash_seed = 666

order_id = input("enter order_id\n").strip()
expected_data = []
print("\nEnter the data that you expect to be scraped in this format\n\
(hit ENTER without entering text to finish):\n\
article_id,ticket,date,quantity,price\n\
123456,Eintritt regulär,2019-12-31 23:59:00,1,14.0\n")
while True:
    new_data = input().strip()
    if not new_data:
        break
    expected_data.append(new_data)

print("\nAdd this to scrape_order_contains_data.csv in test_data:")
for data in expected_data:
    hashed = mmh3.hash(data, seed=hash_seed)
    print(f"{order_id},{hashed}")
