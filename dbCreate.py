import sqlite3
import pandas as pd


# Global database variable
foods = ['FMID', 'Bakedgoods', 'Cheese', 'Crafts',
         'Flowers', 'Eggs', 'Seafood', 'Herbs',
         'Vegetables', 'Honey', 'Jams', 'Maple',
         'Meat', 'Nursery', 'Nuts', 'Plants',
         'Poultry', 'Prepared', 'Soap', 'Trees',
         'Wine', 'Coffee', 'Beans', 'Fruits',
         'Grains', 'Juices', 'Mushrooms', 'PetFood',
         'Tofu', 'WildHarvested']

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
        '''
        Commit the changes and close the connection to the database
        '''
        print("Cleaning Up")
        self.conn.commit()
        self.conn.close()

    def insert_MarketName(self, dataframe):
        '''
        Functon to create the selection from the dataframe
        and insert the the rows into the MarketName table
        Args: Dataframe that will be input into the Db
        Returns: Nothing
        '''
        sql = ''' INSERT INTO MarketName(FMID,MarketName,updateTime)
            VALUES(?,?,?) '''

        market_tuples = dataframe[['FMID', 'MarketName', 'updateTime']]
        market_tuples = [tuple(x) for x in market_tuples.values]
        for row in market_tuples:
            self.c.execute(sql, row)

    def insert_SocialMedia(self, dataframe):
        '''
        Functon to create the selection from the dataframe
        and insert the the rows into the Social media table
        Args: Dataframe that will be input into the Db
        Returns
        '''
        sql = ''' INSERT INTO SocialMedia(FMID,SMID,value)
            VALUES(?,?,?) '''

        social_media_tuples = None
        for column in social_media:
            temp = dataframe[['FMID', column]]
            temp = temp[temp[column].notnull()]
            temp.insert(1, 'SMID', social_media_map[column])
            temp = [tuple(x) for x in temp.values]
            if social_media_tuples is None:
                social_media_tuples = temp
            else:
                social_media_tuples.extend(temp)
        for row in social_media_tuples:
            self.c.execute(sql, row)

    def insert_Time(self, dataframe):
        '''
        Functon to create the selection from the dataframe
        and insert the the rows into the Time table
        Args: Dataframe that will be input into the Db
        Returns
        '''
        sql = ''' INSERT INTO Time(FMID,TID,value)
            VALUES(?,?,?) '''

        time_tuples = None
        for column in timeing:
            temp = dataframe[['FMID', column]]
            temp = temp[temp[column].notnull()]
            temp.insert(1, 'TID', timing_map[column])
            temp = [tuple(x) for x in temp.values]
            if time_tuples is None:
                time_tuples = temp
            else:
                time_tuples.extend(temp)
        for row in time_tuples:
            self.c.execute(sql, row)

    def insert_Foods(self, dataframe):
        sql = ''' INSERT INTO Foods(FMID , Bakedgoods , Cheese , Crafts ,
            Flowers , Eggs ,Seafood , Herbs ,
            Vegetables , Honey , Jams , Maple ,
            Meat , Nursery , Nuts ,Plants ,
            Poultry , Prepared , Soap , Trees ,
            Wine , Coffee ,Beans , Fruits ,
            Grains , Juices , Mushrooms , PetFood ,
            Tofu ,WildHarvested )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''

        food_tuples = dataframe[foods]
        food_tuples = [tuple(x) for x in food_tuples.values]
        for row in food_tuples:
            self.c.execute(sql, row)

    def insert_Location(self, dataframe):
        sql = ''' INSERT INTO Location(FMID, x, y, street, city,
                         County, StateCode, zip, Location)
            VALUES(?,?,?,?,?,?,?,?,?) '''
        location_cols = ['FMID', 'x', 'y', 'street', 'city',
                         'County', 'StateCode', 'zip', 'Location']
        location_tuples = dataframe[location_cols]
        location_tuples = [tuple(x) for x in location_tuples.values]
        for row in location_tuples:
            self.c.execute(sql, row)

    def insert_Payment(self, dataframe):
        sql = ''' INSERT INTO Payment(FMID,PID,value)
            VALUES(?,?,?) '''

        payment_tuples = None
        for column in payments:
            temp = dataframe[['FMID', column]]
            temp = temp[temp[column].notnull()]
            temp.insert(1, 'PID', payment_map[column])
            temp = [tuple(x) for x in temp.values]
            if payment_tuples is None:
                payment_tuples = temp
            else:
                payment_tuples.extend(temp)
        for row in payment_tuples:
            self.c.execute(sql, row)

    def create_tables(self):
        '''
        Create the relevant database tables following the ER schema
        '''
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

    db.insert_MarketName(dataframe)
    db.insert_SocialMedia(dataframe)
    db.insert_Time(dataframe)
    db.insert_Foods(dataframe)
    db.insert_Location(dataframe)
    db.insert_Payment(dataframe)

    db.clean_up()
