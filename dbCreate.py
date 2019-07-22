import sqlite3
import sqlalchemy
import pandas as pd


# Global database variable


social_media = ['Website', 'Facebook', 'TwitterHandle',
                'Youtube', 'Instagram', 'OtherMedia']

social_media_map = dict(zip(social_media, range(len(social_media))))
print(social_media_map)

timeing = ['Season1Date', 'Season1Time', 'Season2Date',
           'Season2Time', 'Season3Date', 'Season3Time',
           'Season4Date', 'Season4Time']

timing_map = dict(zip(timeing, range(len(timeing))))
print(timing_map)

payments = ['Credit', 'WIC', 'WICcash', 'SFMNP',
            'SNAP']
payment_map = dict(zip(payments, range(len(payments))))

print(payment_map)


class Database():
    '''
    Creates the database and runs appropriate 
    functions and implements update functionality
    '''

    def __init__(self, path):
        self.db_path = path
        # Create the database and connect to it
        self.conn = sqlite3.connect(path)
        # Cursor object used to db manipulation
        self.c = self.conn.cursor()

    def clean_up(self):
        print("Cleaning Up")
        self.conn.commit()
        self.conn.close()

    def create_tables(self):
            # Create MarketName table
        self.c.execute('''CREATE TABLE IF NOT EXISTS MarketName
            (FMID int, MarketName text, updateTime text)''')

        # Create SocialMedia table
        self.c.execute('''CREATE TABLE IF NOT EXISTS SocialMedia
            (FMID int, SMID int, value text)''')

        # Create Foods table
        self.c.execute('''CREATE TABLE IF NOT EXISTS Time
            (FMID int, TID int, value text)''')

        # Create Time table
        self.c.execute('''CREATE TABLE IF NOT EXISTS Foods
            (FMID int, Bakedgoods text, Cheese text, Crafts text,
            Flowers text, Eggs text,Seafood text, Herbs text,
            Vegetables text, Honey text, Jams text, Maple text,
            Meat text, Nursery text, Nuts text,Plants text,
            Poultry text, Prepared text, Soap text, Trees text,
            Wine text, Coffee text,Beans text, Fruits text,
            Grains text, Juices text, Mushrooms text, PetFood text,
            Tofu text,WildHarvested text)''')

        # Create Location table
        self.c.execute('''CREATE TABLE IF NOT EXISTS Location
            (FMID int, x int, y int, street text, city text,
            County int, StateCode text, zip int, Location int)''')

        # Create Payment table
        self.c.execute('''CREATE TABLE IF NOT EXISTS Payment
            (FMID int, PID int, value text)''')
        print("All tables have been made or already exist.")


if __name__ == '__main__':
    dataframe = pd.read_csv('data/clean_data.csv')
    db = Database('database/finalproject.db')
    db.create_tables()
    db.clean_up()

