# tri des données et fusions dans un fichier csv commun
import pandas as pd
import numpy as np
import math
from datetime import datetime
from datetime import date
import category_encoders as ce


def merge_dataframe(file):
    """
        Merge des dataframes de member.csv, restaurant.csv et Train.csv / Test.csv
    """
    df_member = pd.read_csv("member.csv", encoding="UTF-16", sep="\t")
    df_restaurant = pd.read_csv("restaurant.csv", encoding="UTF-16", sep="\t")
    df_train = pd.read_csv(file, encoding="UTF-8", sep=",")

    # renommer certaines colonnes pour les left join
    df_member = df_member.rename(columns={"id":"member_id", "cdate":"cdate_member", "city":"city_member"})
    df_restaurant = df_restaurant.rename(columns={"id":"restaurant_id", "cdate":"cdate_restaurant", "city":"city_restaurant"})
    df_train = df_train.rename(columns={"cdate":"cdate_" + file[:-4].lower()})

    # left join de Train à member
    result = pd.merge(df_train, df_member, on="member_id", how="left")

    # left join de result à restaurant
    df = pd.merge(result, df_restaurant, on="restaurant_id", how="left")


    # drop la latitude et longitude car donnees redondantes
    # drop de name, abbr et tel car donnees anonymisees
    # drop de restaurant_id et member_it car donnees anonymisees
    # drop de cityarea et city_member car trop de données manquantes
    # drop de timezone et locale car données récurentes (on sait déjà que c'est à Taiwan...)
    df = df.drop(["lat", "lng", "name", "abbr", "tel", "cityarea", "city_member", "opening_hours", "timezone", "locale"], axis=1)

    return df


def change_date_format(column):
    """
        Sert à mettre cdate_train et datetime dans le format "MM/JJ/AAAA hh:mm"
    """
    dates = []
    for date in column:
        clean = date.replace(",,"," ")
        clean = clean.replace(","," ")
        split = clean.split(" ")
        if split[2] == "PM":
            hour = str(int(split[1].split(":")[0]) + 12)
            if hour == "24":
                hour = "0"
            new_hour = hour + ":" + split[1].split(":")[1]
            clean = split[0] + " " + new_hour
        else:
            clean = split[0] + " " + split[1].split(":")[0] + ":" + split[1].split(":")[1]
        dates.append(clean)

    return dates


def date_format(df):
    """
        Applique la fonction change_date_format() à cdate_train et datetime
    """
    try:
        df["cdate_train"] = change_date_format(df["cdate_train"])
    except:
        df["cdate_test"] = change_date_format(df["cdate_test"])

    df["datetime"] = change_date_format(df["datetime"])

    return df


def drop_hours(df):
    """
        Drop l'heure et les minutes des dates (cdate_train/test, datetime, cdate_member, cdate_restaurant) pour une question de simplicité
    """

    cdate_train = []
    cdate_test = []
    datetime = []
    cdate_member = []
    cdate_restaurant = []

    try:
        for elt in df["cdate_train"]:
            cdate_train.append(elt.split(" ")[0])
    except:
        for elt in df["cdate_test"]:
            cdate_test.append(elt.split(" ")[0])
    for elt in df["datetime"]:
        datetime.append(elt.split(" ")[0])
    for elt in df["cdate_member"]:
        cdate_member.append(elt.split(" ")[0])
    for elt in df["cdate_restaurant"]:
        cdate_restaurant.append(elt.split(" ")[0])

    try:
        df["cdate_train"] = cdate_train
    except:
        df["cdate_test"] = cdate_test
    df["datetime"] = datetime
    df["cdate_member"] = cdate_member
    df["cdate_restaurant"] = cdate_restaurant

    return df


def split_date(df):
    """
        Sépare les jours, mois et années dans différentes colonnes pour chaque date
    """

    date_list = ['cdate_train', 'datetime', 'birthdate', 'cdate_member', 'cdate_restaurant']


    for column in date_list:
        month = []
        day = []
        year = []
        try:
            for elt in df[column]:
                month.append(float(elt.split("/")[0]))
                day.append(float(elt.split("/")[1]))
                year.append(float(elt.split("/")[2]))
        except:
            for elt in df["cdate_test"]:
                month.append(float(elt.split("/")[0]))
                day.append(float(elt.split("/")[1]))
                year.append(float(elt.split("/")[2]))
            df["cdate_test_month"] = month
            df["cdate_test_day"] = day
            df["cdate_test_year"] = year

        df[column + "_month"] = month
        df[column + "_day"] = day
        df[column + "_year"] = year

    return df


def birthdate_clean(df):
    """
        Clean les données de birthdate et remplit les NaN par la moyenne
    """
    birthdates = []
    for birthdate in df["birthdate"]:
        # si la date est un float, on remplace par un NaN
        if type(birthdate) == float:
            birthdate = "00/00/0000"
        # on convertit les date en "0000-00-00" par "00/00/0000"
        if birthdate == "0000-00-00":
            birthdate = "00/00/0000"
        # date de la forme "0054-00-00", on interprête par "01/01/1954"
        if "-" in birthdate and birthdate.split("-")[0][:2] == "00" and birthdate.split("-")[0][-2:] != "00":
            birthdate = "1/1/19" + birthdate.split("-")[0][-2:]
        # date de la forme "1971-00-00", on interprête par "01/01/1971"
        if "-" in birthdate:
            birthdate = "1/1/" + birthdate.split("-")[0]
        # NaN pour toutes les valeurs de la forme "0000-00-00"
        if birthdate.split("/")[2] == "0000":
            birthdate = np.nan
        birthdates.append(birthdate)
    df["birthdate"] = birthdates

    # calcul de l'age par rapport à la date
    today = date.today()
    age_dropna = []
    for elt in df["birthdate"].dropna():
        datetime_object = datetime.strptime(elt, '%m/%d/%Y')
        age = today.year - datetime_object.year - ((today.month, today.day) < (datetime_object.month, datetime_object.day))
        age_dropna.append(age)

    # moyenne d'age
    age_mean = round(np.mean(age_dropna))
    # conversion de l'age en date, sous la forme 01/01/YYYY
    date_mean = "1/1/" + str(int(date.today().year) - int(age_mean))
    df["birthdate"] = df["birthdate"].fillna(date_mean)

    return df


def categorial_to_ordinal(df):
    """
        Transforme les données catégorielles (purpose, gender_x, status, gender_y, country, currency, city_restaurant) en données ordinales
    """

    categorial_list = ['purpose', 'gender_x', 'status', 'gender_y', 'country', 'currency', 'city_restaurant']

    for column in categorial_list:
        ce_ord = ce.OrdinalEncoder(cols = [column])
        df["ord_" + column] = ce_ord.fit_transform(df[column])

    return df


def city_member_to_nan(df):
    """
        Transforme correctement les données en NaN
    """
    cities = []
    for city in df["city_member"]:
        if city == "0":
            city = np.nan
        cities.append(city)

    df["city_member"] = cities

    return df


def is_nan(x):
    """
        Retourne True si x == NaN, False sinon
    """
    return isinstance(x, float) and math.isnan(x)


def number_of_nan(df):
    """
        Retourne un dict du nombre de NaN pour chaque colonne
    """
    dict = {}
    columns = list(df.columns)
    for column in columns:
        i = 0
        for elt in df[column]:
            if is_nan(elt):
                i += 1
        dict[column] = i

    return dict


def int_to_float(df):

    people = []
    prepay = []
    return90 = []

    for elt in df["people"]:
        people.append(float(elt))
    for elt in df["is_required_prepay_satisfied"]:
        prepay.append(float(elt))
    try:
        for elt in df["return90"]:
            return90.append(float(elt))
    except:
        pass

    df["people"] = people
    df["is_required_prepay_satisfied"] = prepay
    try:
        df["return90"] = return90
    except:
        pass

    return df


if __name__ == '__main__':
    df_train = merge_dataframe("Train.csv")
    df_test = merge_dataframe("Test.csv")

####################### TRAIN ###########################

    df_train = date_format(df_train)
    df_train = birthdate_clean(df_train)
    # df_train = city_member_to_nan(df_train)
    df_train = df_train.dropna()
    df_train = int_to_float(df_train)
    df_train = drop_hours(df_train)
    df_train = split_date(df_train)
    df_train = categorial_to_ordinal(df_train)
    # print(Index(df_train["city_member"].value_counts()).get_loc("9407"))

    # print(type(df_train["ord_purpose"][0]))


####################### TEST ###########################

    df_test = date_format(df_test)
    df_test = birthdate_clean(df_test)
    # df_test = city_member_to_nan(df_test)
    df_test = df_test.dropna()
    df_test = int_to_float(df_test)
    df_test = drop_hours(df_test)
    df_test = split_date(df_test)
    df_test = categorial_to_ordinal(df_test)

########################################################

    # for k,v in number_of_nan(df).items():
    #     print(k, v)

    # print(df_train)
    # print("--------------------------")
    # print(df_test)
    df_train.to_csv("./df_train.csv", index=False)
    df_test.to_csv("./df_test.csv", index=False)
