import pandas as pd
import numpy as np
import re
import os
from datetime import datetime
from textwrap import wrap
from langdetect import detect


class Preprocessing_Gratka:
    """
        A class used to preprocess offers information from Gratka.pl.
        ...
        Attributes
        ----------
        apartment_details: name of apartments table
        information_types: columns of apartments table
        Methods
        -------
        remove_quotation_marks() -> pd.DataFrame:
            Remove quotation marks from columns.
        numeric_information() -> pd.DataFrame:
            Replace numeric information to float.
        remove_new_line_marks() -> pd.DataFrame:
            Remove new line marks from columns.
        prepare_table_information(table) -> pd.DataFrame:
            Change table information from list to dictionary and create external table from it.
        get_number(price_information: str) -> str:
            Get only numeric information of price from string.
        extract_price(apartment_details_price_table) -> pd.DataFrame:
            Extract price and currency from a string and convert price to float.
        prepare_additional_info(apartment_details_add_info_table, apartment_details_details_table) -> pd.DataFrame:
            Join additional information and details to additional information.
        prepare_description_table(apartment_details_description_table: pd.DataFrame) -> pd.DataFrame:
            Split description of 4000 characters to several columns and if record has more than 16000 characters check the language and get only polish description.
        create_table() -> pd.DataFrame:
            Create final preprocessing table.
        """

    def __init__(self, apartment_details, information_types):
        """
        Parameters
        ----------
        apartment_details : PandasDataFrame
            name of apartments table.
        information_types : str
            columns of apartments table.
        """
        self.apartment_details = apartment_details
        self.information_types = information_types

    def extract_address(self, location_table):
        apartment_details = self.apartment_details
        prepared_table = []
        for i in range(len(apartment_details)):
            try:
                prepared_table.append(eval(location_table.str.strip(": ")[i])[0])
            except:
                prepared_table.append(None)

        address_table = pd.DataFrame()
        for i in range(len(prepared_table)):
            column = []
            row = []
            try:
                for key, value in prepared_table[i].items():
                    column.append(key.strip(':'))
                    row.append(value)
            except:
                row.append(None)
            df_temp = pd.DataFrame([row], columns=column)
            address_table = pd.concat([address_table, df_temp], ignore_index=True)

        return address_table

    def prepare_table_information(self, table: pd.DataFrame) -> pd.DataFrame:
        """Change table information from list to dictionary and create external table from it.
        Parameters
        ----------
        table: pd.DataFrame
            column with table information.
        Returns
        ------
        prepared_tables: pd.DataFrame
            data frame with table information.
        """
        add_info = []
        df1 = pd.DataFrame()
        for index, value in enumerate(table):
          a =[]
          df = pd.DataFrame.from_dict(value)
          transposed = df.T.reset_index(drop=True)
          colnames = transposed.iloc[0,:]
          df2 = transposed.iloc[1:,:].reset_index(drop=True)
          data1 = pd.DataFrame(df2.values, columns=colnames)
          df1 = pd.concat([df1, data1], axis=0).reset_index(drop=True)
          [a.append(str(colnames[i]) + ": " + str(df2.values[0][i])) for i in range(data1.shape[1])]
          add_info.append(a)

        return add_info, df1.where(pd.notnull(df1), None)

    def prepare_description_table(self, apartment_details_description_table: pd.DataFrame) -> pd.DataFrame:
        """Split description of 4000 characters to several columns and if record has more than 16000 characters check the language and get only polish description.
        Parameters
        ----------
        apartment_details_description_table: pd.DataFrame
            column with description.
        Returns
        ------
        description_1: List
            list with tha first part of description.
        """
        description_1 = []
        description_2 = []
        description_3 = []
        description_4 = []

        for i in range(len(apartment_details_description_table)):
            desc_list = [None, None, None, None]
            if apartment_details_description_table[i] == None:
                description_splitted = None
            elif len(apartment_details_description_table[i]) > 16000:

                description = apartment_details_description_table[i]
                text = " ".join(description).replace("\xa0", " ").strip()
                elements = [text[x:x + 6] for x in range(0, len(text), 6)]

                pl = []
                for index, element in enumerate(elements):
                    element = list(map(str.lower, element))
                    try:
                        language = detect(" ".join(element))
                    except:
                        language = 'pl'
                    if language == 'pl':
                        pl.append(" ".join(element))

                description_splitted = wrap(" ".join(pl), 4000)

            else:
                try:
                    description_splitted = wrap(
                        "".join(apartment_details_description_table[i]).replace("\xa0", " ").strip(), 4000)
                except:
                    description_splitted = wrap(''.join(apartment_details_description_table[i]), 4000)

            try:
                for element in range(len(description_splitted)):
                    desc_list[element] = description_splitted[element]
            except:
                desc_list[element] = None

            description_1.append(desc_list[0])
            description_2.append(desc_list[1])
            description_3.append(desc_list[2])
            description_4.append(desc_list[3])

        self.apartment_details['description_2'] = description_2
        self.apartment_details['description_3'] = description_3
        self.apartment_details['description_4'] = description_4

        return description_1

    def handle_exception(self, table):
        val = []
        for i in range(len(table)):
            try:
                val.append(table[i])
            except:
                val.append(None)
        return val

    def handle_exception_area(self, area_table):
        area = []
        for i in range(len(area_table)):
            try:
                area.append(float(area_table[i].split()[0].replace(",", ".")))
            except:
                area.append(None)
        return area

    def handle_exception_add_info(self, add_info_gratka):
        add_info = []
        for i in range(len(add_info_gratka)):
            try:
                add_info.append(str(add_info_gratka[i]).replace("'","").replace("[","").replace("]",""))
            except:
                add_info.append(None)
        return add_info

    def handle_exception_price(self, price_table):
        price = []
        for i in range(len(price_table)):
            try:
                price.append(float(price_table[i].replace(" ", "").replace(",", ".")))
            except:
                price.append(None)
        return price

    def handle_exception_currency(self, currency_table):
        currency = []
        for i in range(len(currency_table)):
            try:
                currency.append(currency_table[i].split("/")[0])
            except:
                currency.append(None)
        return currency

    def handle_exception_title(self, title_table):
        title = []
        for i in range(len(title_table)):
            try:
                title.append(title_table[i][0])
            except:
                title.append(None)
        return title

    def create_table(self):
        """Create final preprocessing table.
        Returns
        ------
        otodom_table: pd.DataFrame
            final preprocessing table with None instead of empty strings.
        """
        gratka_table = pd.DataFrame()
        address = self.extract_address(location_table=self.apartment_details['location_params'])
        add_info_gratka, params_tables_gratka = self.prepare_table_information(table=self.apartment_details['details'])
        gratka_table["area"] = self.handle_exception_area(area_table=params_tables_gratka["Powierzchnia w m2"])
        gratka_table["latitude"] = self.handle_exception(table=address["lokalizacja_szerokosc-geograficzna-y"])
        gratka_table["longitude"] = self.handle_exception(table=address["lokalizacja_dlugosc-geograficzna-x"])
        gratka_table["link"] = self.handle_exception(table=self.apartment_details.link)
        gratka_table["price"] = self.handle_exception_price(price_table=self.apartment_details.price)
        gratka_table["currency"] = self.handle_exception_currency(currency_table=self.apartment_details.price_currency)
        gratka_table["rooms"] = self.handle_exception(table=params_tables_gratka["Liczba pokoi"])
        gratka_table["floors_number"] = self.handle_exception(table=params_tables_gratka["Liczba pięter w budynku"])
        gratka_table["floor"] = self.handle_exception(table=params_tables_gratka["Piętro"])
        gratka_table["type_building"] = self.handle_exception(table=params_tables_gratka["Typ zabudowy"].str.lower())
        gratka_table["material_building"] = self.handle_exception(
            table=params_tables_gratka["Materiał budynku"].str.lower())
        gratka_table["year"] = self.handle_exception(table=params_tables_gratka["Rok budowy"])
        gratka_table["headers"] = None
        gratka_table["additional_info"] = self.handle_exception_add_info(add_info_gratka)
        gratka_table['city'] = self.handle_exception(table=address["lokalizacja_miejscowosc"])
        gratka_table['address'] = self.handle_exception(table=address["lokalizacja_ulica"])
        gratka_table['district'] = self.handle_exception(table=address["lokalizacja_dzielnica"])
        gratka_table['voivodeship'] = self.handle_exception(table=address["lokalizacja_region"])
        gratka_table['active'] = 'Yes'
        gratka_table['scrape_date'] = str(datetime.now().date())
        gratka_table['inactive_date'] = '-'
        gratka_table['page_name'] = 'Gratka'
        gratka_table['offer_title'] = self.handle_exception_title(title_table=self.apartment_details.title)
        gratka_table['description_1'] = self.prepare_description_table(self.apartment_details['description'])
        gratka_table['description_2'] = self.apartment_details['description_2']
        gratka_table['description_3'] = self.apartment_details['description_3']
        gratka_table['description_4'] = self.apartment_details['description_4']
        return gratka_table.replace({"": None})
