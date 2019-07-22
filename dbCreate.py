import sqlite3
import sqlalchemy
import pandas as pd


# Load in the previously created clean_dataset
dataframe = pd.read_csv('clean_data.csv')

# Create the database and connect to it
conn = sqlite3.connect('finalproject.db')

# Cursor object used to db manipulation
c = conn.cursor()

social_media = ['Website', 'Facebook', 'TwitterHandle',
                'Youtube', 'Instagram', 'OtherMedia']

social_media_map = dict(zip(social_media, range(len(social_media))))
print(social_media_map)

timeing = ['Season1Date', 'Season1Time', 'Season2Date'
           'Season2Time', 'Season3Date', 'Season3Time'
           'Season4Date', 'Season4Time']

timing_map = dict(zip(timeing, range(len(timeing))))
print(timing_map)

payments = ['Credit', 'WIC', 'WICcash', 'SFMNP'
            'SNAP']
payment_map = dict(zip(payments, range(len(payments))))

print(payment_map)


if __name__ == '__main__':
    # Create MarketName table
    c.execute('''CREATE TABLE IF NOT EXISTS MarketName
        (FMID int, MarketName text, updateTime text)''')
    print('Created MarketName Table')

    # Create SocialMedia table
    c.execute('''CREATE TABLE IF NOT EXISTS SocialMedia
        (FMID int, SMID int, value text)''')
    print('Created SocialMedia Table')

    # Create Foods table
    c.execute('''CREATE TABLE IF NOT EXISTS Time
        (FMID int, TID int, value text)''')
    print('Created Foods Table')

    # Create Time table
    c.execute('''CREATE TABLE IF NOT EXISTS Foods
        (FMID int, Bakedgoods text, Cheese text, Crafts text,
        Flowers text, Eggs text,Seafood text, Herbs text,
        Vegetables text, Honey text, Jams text, Maple text,
        Meat text, Nursery text, Nuts text,Plants text,
        Poultry text, Prepared text, Soap text, Trees text,
        Wine text, Coffee text,Beans text, Fruits text,
        Grains text, Juices text, Mushrooms text, PetFood text,
        Tofu text,WildHarvested text)''')
    print('Created Time Table')

    # Create Location table
    c.execute('''CREATE TABLE IF NOT EXISTS Location
        (FMID int, x int, y int, street text, city text,
        County int, StateCode text, zip int, Location int)''')
    print('Created Location Table')

    # Create Payment table
    c.execute('''CREATE TABLE IF NOT EXISTS Payment
        (FMID int, PID int, value text)''')
    print('Created Payment Table')


    print("Clean up")
    conn.commit()
    conn.close()
