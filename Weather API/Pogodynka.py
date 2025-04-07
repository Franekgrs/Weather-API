import urllib
import matplotlib
import pandas as pd
import requests
import sqlalchemy
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from urllib.parse import quote_plus



class Pogodynka:
    """
    A class that runs whole Pogodynka app. It contains a few methods which
    provides whole application logic.
    """
    def __init__(self, connection_string):
        """
        Constructor
        :param connection_string:
        """
        self.engine = create_engine(connection_string)
        self.df_from_api = self.wczytajDane("https://danepubliczne.imgw.pl/api/data/synop/")

    def wczytajDane(self, url):
        """
        Loading data into dataframe (pandas dataframe)
        :param  url (string): url to load json data from website
        :return:
            returns dataframe filled with weather data
        """
        response = requests.get(url)
        data = response.json()
        df = pd.DataFrame(data)
        return df

    def dodajDaneDoBazy(self, df, table_name):
        """
        Load data into MSSQL database from pandas dataframe
        :param df (dataframe) : dataframe to add
        :param table_name: table name in database
        :return:
        """
        conn = self.engine.connect()

        if not self.czyObecneDaneIstniejaWBazie(df, table_name, conn):
            df.to_sql(table_name, self.engine, if_exists='append', index=False)
            print('Pomyślnie dodano do bazy danych.')
        else:
            print("Aktualne dane znajdują się już w Bazie Danych")
        conn.close()
    def czyTabelaIstniejeWBazie(self,tabela,conn):
        """
        Checking if table with that name exist in database
        :param tabela: table name in database
        :param conn: connecting engine to database
        :return: Boolean if table with that name exist in database
        """
        query0 = f"""
                DECLARE @tableExists BIT;

                IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tabela}')
                BEGIN
                    SET @tableExists = 1;
                END
                ELSE
                BEGIN
                    SET @tableExists = 0;
                END

                SELECT @tableExists AS TableExists;
                """
        result = pd.read_sql_query(query0, conn)
        table_exists = result.loc[0, 'TableExists']

        if not table_exists:
            return False
        return True
    def czyObecneDaneIstniejaWBazie(self, df, tabela, conn):
        """
        If true, then checking if table already contains current data.
        :param df: dataframe to check
        :param tabela: table name in database
        :param conn: connecting engine to database
        :return: boolean if current data exist in database
        """

        ifTableExist = self.czyTabelaIstniejeWBazie(tabela,conn)
        if not ifTableExist:
            return False

        query = f"SELECT * FROM {tabela}"
        existing_df = pd.read_sql(query, conn)

        if existing_df.empty:
            return False

        for _, row in df.iterrows():
            if not existing_df[(existing_df['stacja'] == row['stacja']) &
                               (existing_df['data_pomiaru'] == row['data_pomiaru']) &
                               (existing_df['godzina_pomiaru'] == row['godzina_pomiaru'])].empty:
                return True
        return False

    def WyswietlPogodeDlaStacji(self, df, stacja):
        """
        Printing current weather data for given station.
        :param df: dataframe containing weather data
        :param stacja: station name
        :return:
        """
        station_data = df[df['stacja'] == stacja]
        if not station_data.empty:
            for index, row in station_data.iterrows():
                print(f"Stacja: {row['stacja']} ({row['id_stacji']}):")
                print(f"Temperatura: {row['temperatura']}°C")
                print(f"Predkosc wiatru: {row['predkosc_wiatru']} m/s")
                print(f"Kierunek Wiatru: {row['kierunek_wiatru']}°")
                print(f"Wilgotnosc wzgledna: {row['wilgotnosc_wzgledna']}%")
                print(f"Suma opadu: {row['suma_opadu']} mm")
                print(f"Cisnienie atmosferyczne: {row['cisnienie']} hPa")
                print(f"Data: {row['data_pomiaru']}")
                print(f"Godzina: {row['godzina_pomiaru']}\n")
        else:
            print(f"Nie znaleziono danych dla stacji: {stacja}")

    def wyswietlOstrzerzeniaDlaStacji(self, df, stacja):
        """
        Printing weather warnings for given station.
        :param df: dataframe containing weather data
        :param stacja: station name
        :return:
        """
        station_data = df[df['stacja'] == stacja]
        if not station_data.empty:
            for index, row in station_data.iterrows():
                print("Ostrzerzenia Pogodowe: ")
                if (float(row['temperatura']) < 0):
                    print("Temperatura poniżej zera")
                if (float(row['temperatura']) < 10):
                    print("Temperatura poniżej 10 stopni")
                if (float(row['temperatura']) > 28):
                    print("Temperatura wynosi powyżej 28 stopni.")
                if (float(row['predkosc_wiatru']) > 1):
                    print("Predkość wiatru jest wysoka.")
                if (float(row['suma_opadu']) > 0):
                    print("Występuje deszcz.")
                if (float(row['wilgotnosc_wzgledna']) > 50):
                    print("Wilgotnosc wzgledna powietrza wynosi powyżej 50%")
        else:
            print(f"Nie znaleziono danych dla stacji: {stacja}")

    def wyswietlWykresHistoriiTemparaturDlaStacji(self, tabela, station_name):
        """
        Showing historical data plot kept in databse
        :param tabela: table name
        :param station_name: station name
        :return:
        """


        query = f"Select * from {tabela} where stacja = '{station_name}'"
        df = pd.read_sql_query(query, self.engine)
        df['data_pomiaru'] = df['data_pomiaru'].astype(str) + '\n ' + df['godzina_pomiaru'].astype(str) + ":00"
        df = df.set_index('data_pomiaru')


        wybor = int(input("Lista dostępnych parametrów:\n"
                         "1. Temperatura\n"
                         "2. Predkosc wiatru\n"
                         "3. Cisnienie\n"
                         "4. Kierunek Wiatru\n"
                         "5. Suma opadu\n"
                         "6. Wilgotnosc wzgledna\n"
                         "Wprowadz paramertr: "))

        if wybor == 1:
            parametr = "temperatura"
        elif wybor == 2:
            parametr = "predkosc_wiatru"
        elif wybor == 3:
            parametr = "cisnienie"
        elif wybor == 4:
            parametr = "kierunek_wiatru"
        elif wybor == 5:
            parametr = "suma_opadu"
        elif wybor == 6:
            parametr = "wilgotnosc_wzgledna"
        else:
            parametr = "temperatura"



        df[parametr] = df[parametr].astype(float)


        plt.figure(figsize=(10, 6))
        plt.scatter(df.index, df[parametr],label=parametr, color='red')
        plt.xlabel('Data Pomiaru')
        plt.ylabel(parametr)
        plt.title(f'{parametr} dla {station_name}')
        plt.legend()
        plt.grid(True)

        plt.tight_layout()
        plt.show()

    def obliczSrednieParametrow(self, df):
        """
        Printing current average weather parameters for the whole Country.
        :param df: dataframe with current weather data
        :return:
        """
        columns_to_convert = ['temperatura', 'predkosc_wiatru', 'kierunek_wiatru', 'wilgotnosc_wzgledna', 'suma_opadu',
                              'cisnienie']

        for col in columns_to_convert:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        mean_temperatura = df['temperatura'].mean()
        mean_predkosc_wiatru = df['predkosc_wiatru'].mean()
        mean_kierunek_wiatru = df['kierunek_wiatru'].mean()
        mean_wilgotnosc_wzgledna = df['wilgotnosc_wzgledna'].mean()
        mean_suma_opadu = df['suma_opadu'].mean()
        mean_cisnienie = df['cisnienie'].mean()

        # Wyświetlanie wyników
        print(f"Średnia temperatura: {mean_temperatura}°C")
        print(f"Średnia prędkość wiatru: {mean_predkosc_wiatru} m/s")
        print(f"Średni kierunek wiatru: {mean_kierunek_wiatru}°")
        print(f"Średnia wilgotność względna: {mean_wilgotnosc_wzgledna}%")
        print(f"Średnia suma opadu: {mean_suma_opadu} mm")
        print(f"Średnie ciśnienie atmosferyczne: {mean_cisnienie} hPa")

    def menu(self):
        """
        Logic of the application. Taking user inputs to run the application
        :return:
        """
        self.dodajDaneDoBazy(self.df_from_api, 'Pogodatest')
        if_continue = True

        while if_continue:
            print("---------------------------------------------\n\n\n"
                  "Wybierz operację do przeprowadzenia \n"
                  "1. Wyswietl obecne dane pogodowe dla wszystkich stacji \n"
                  "2. Wyswietl obecne dane pododowe dla podanej stacji \n"
                  "3. Wyswietl historyczne dane pogodowe dla wybranej stacji \n"
                  "4. Wyswietl usrednione dane dla wszystkich stacji\n"
                  "5. Wyswietl ostrzerzenia pogodowe dla stacji\n"
                  "6. Odśwież dane pogodowe\n"
                  "0. Zakoncz dzialanie programu\n"
                  "---------------------------------------------")

            try:
                decyzja = int(input("Wprowadz wybraną opcję z menu jako liczba: "))
                print("-----------------------------------------------\n\n")

                if decyzja == 1:
                    print(self.df_from_api.to_string())
                elif decyzja == 2:
                    stacja = input("Podaj stację: ")
                    self.WyswietlPogodeDlaStacji(self.df_from_api, stacja)
                elif decyzja == 3:
                    stacja = input("Podaj stację: ")
                    self.wyswietlWykresHistoriiTemparaturDlaStacji("PogodaTest", stacja)
                elif decyzja == 4:
                    self.obliczSrednieParametrow(self.df_from_api)
                elif decyzja == 5:
                    stacja = input("Podaj stację: ")
                    self.wyswietlOstrzerzeniaDlaStacji(self.df_from_api, stacja)
                elif decyzja == 6:
                    self.df_from_api = self.wczytajDane("https://danepubliczne.imgw.pl/api/data/synop/")
                    self.dodajDaneDoBazy(self.df_from_api, 'Pogodatest')
                elif decyzja == 0:
                    if_continue = False
                else:
                    print("Niepoprawny wybór... Spróbuj ponownie")

            except ValueError:
                print("Wprowadzona wartość musi być liczbą całkowitą. Spróbuj ponownie.")


if __name__ == "__main__":
    ##tutaj odbywa się łączenie do serwera mssql za pomocą parametrów
    connection_string = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};SERVER=db-mssql16.pjwstk.edu.pl;DATABASE=2019SBD;Trusted_Connection=yes;")


    driver = "ODBC Driver 17 for SQL Server"
    server = "localhost"
    database = "master"
    username = "sa"
    password = "Password2424%%"

    connection_string_local = "mssql+pyodbc:///?odbc_connect=" + quote_plus(
        f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    )

    app = Pogodynka(connection_string_local)
    app.menu()


