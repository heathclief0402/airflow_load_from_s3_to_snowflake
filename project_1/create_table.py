
import random
from faker import Faker
import pandas as pd


fake = Faker()

def generate_mock_data(date, num_rows=100):
    categories = ["Electronics", "Clothing", "Home", "Beauty", "Toys", "tools", "cars"]
    regions = ["US", "EU", "Asia", "South America", "North America"]
    statuses = ["Shipped", "Pending", "Delivered", "Returned"]
    product_name = ["Alpha Pro","Solar Max","Pulse X","Vision Lite","Quantum Plus","Echo Ultra","Zenith Elite","Nova Basic"
                        ,"Titan Edge","Spectrum Air","Fusion Core""Stratos X"
                        ,"Hyper Nova","Vortex Max","Sonic Beam","Nimbus Lite","Astra Prime"
                        ,"Neon Plus","Optima Ultra","Cyber Shield"]
    data = []
    for i in range(num_rows):
        price = round(random.uniform(10, 1000), 2)
        quantity = random.randint(1, 20)
        discount = round(random.uniform(0, 0.3), 2)
        total = round(price * quantity * (1 - discount), 2)
        #product_name = fake.name() + random.choice(["Pro", "Max", "X", "Lite", "Basic", "Plus"])
        
        row = {
            "id": i + 1,
            "name": fake.company(),
            "product_name": random.choice(product_name),
            "category": random.choice(categories),
            "price": price,
            "quantity": quantity,
            "date": date,
            "region": random.choice(regions),
            "status": random.choice(statuses),
            "discount": f"{round(discount*100,0)}%",
            "total": total
        }
        data.append(row)
    df = pd.DataFrame(data)
    return df

dates = ["2025-03-12", "2025-03-13", "2025-03-14"]
group_name = "team2"
for date in dates: 
    df = generate_mock_data(date)
    filename = f"sale_data_{group_name}_{date}.csv"
    df.to_csv(filename, index = False)
    print(f'sucessfully generate sale data for {date}')
