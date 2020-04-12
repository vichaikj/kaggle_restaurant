# tri des données et fusions dans un fichier csv commun
import pandas as pd
import numpy as np
import math
from datetime import datetime
from datetime import date
from translate import Translator


def merge_dataframe():
    """
        Merge des dataframes de member.csv, restaurant.csv et Train.csv
    """
    df_member = pd.read_csv("member.csv", encoding="UTF-16", sep="\t")
    df_restaurant = pd.read_csv("restaurant.csv", encoding="UTF-16", sep="\t")
    df_train = pd.read_csv("Train.csv", encoding="UTF-8", sep=",")

    # renommer certaines colonnes pour les left join
    df_member = df_member.rename(columns={"id":"member_id", "cdate":"cdate_member", "city":"city_member"})
    df_restaurant = df_restaurant.rename(columns={"id":"restaurant_id", "cdate":"cdate_restaurant", "city":"city_restaurant"})
    df_train = df_train.rename(columns={"cdate":"cdate_train"})

    # left join de Train à member
    result = pd.merge(df_train, df_member, on="member_id", how="left")

    # left join de result à restaurant
    df = pd.merge(result, df_restaurant, on="restaurant_id", how="left")

    # on enlève les NaN, ...
    # df = df.dropna()

    # drop la latitude et longitude car donnees redondantes
    # drop de name, abbr et tel car donnees anonymisees
    # drop de restaurant_id et member_it car donnees anonymisees
    # drop de cityarea car trop de données manquantes
    df = df.drop(["lat", "lng", "name", "abbr", "tel", "cityarea"], axis=1)

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
    df["cdate_train"] = change_date_format(df["cdate_train"])
    df["datetime"] = change_date_format(df["datetime"])

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


def translate(column):
    """
        Traduit le texte dans la colonne spécifiée
    """
    translator = Translator(to_lang="French")

    i = 0
    for text in df[column]:
        text = translator.translate(str(text))
        i += 1
        if text.split(" ")[0] == "MYMEMORY" :
            print("Limite atteinte %s")
            break

    return df[column]


def translate_chinese(df):
    """
        Traduit les colonnes city_member, city_restaurant et opening_hours
    """
    # ne traduire qu'une seule fois chaque récurrence
    df["city_member"] = translate("city_member")

    # df["city_restaurant"] = translate("city_restaurant")
    # df["opening_hours"] = translate("opening_hours")

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


if __name__ == '__main__':
    df = merge_dataframe()
    df = date_format(df)
    df = birthdate_clean(df)
    df = df.dropna()
    # df = translate_chinese(df)


    # for k,v in number_of_nan(df).items():
    #     print(k, v)

    print(df)
    df.to_csv("./df.csv", index=False)
