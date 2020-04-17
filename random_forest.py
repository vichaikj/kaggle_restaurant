import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import LabelEncoder

df_train = pd.read_csv("df_train.csv", encoding="UTF-8", sep=",")
df_test = pd.read_csv("df_test.csv", encoding="UTF-8", sep=",")


X = df_train[['cdate_train_month', 'cdate_train_day', 'cdate_train_year', 'datetime_month', 'datetime_day', 'datetime_year', 'people', 'ord_purpose', 'ord_gender_x', 'ord_status',
'is_required_prepay_satisfied', 'is_vip', 'ord_gender_y', 'birthdate_month', 'birthdate_day', 'birthdate_year', 'has_google_id', 'has_yahoo_id', 'has_weibo_id',
'cdate_member_month', 'cdate_member_day', 'cdate_member_year', 'is_hotel', 'ord_country', 'ord_currency', 'ord_city_restaurant', 'good_for_family', 'accept_credit_card',
'parking', 'outdoor_seating', 'wifi', 'wheelchair_accessible', 'price1', 'price2', 'cdate_restaurant_month', 'cdate_restaurant_day', 'cdate_restaurant_year']]  # Features

y = df_train['return90']  # Labels

# divise la dataset en train et test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)


clf = RandomForestClassifier(n_estimators=100)
clf.fit(X_train,y_train)


# X_test_submit = df_test[['cdate_train_month', 'cdate_train_day', 'cdate_train_year', 'datetime_month', 'datetime_day', 'datetime_year', 'people', 'ord_purpose', 'ord_gender_x', 'ord_status',
# 'is_required_prepay_satisfied', 'is_vip', 'ord_gender_y', 'birthdate_month', 'birthdate_day', 'birthdate_year', 'has_google_id', 'has_yahoo_id', 'has_weibo_id',
# 'cdate_member_month', 'cdate_member_day', 'cdate_member_year', 'is_hotel', 'ord_country', 'ord_currency', 'ord_city_restaurant', 'good_for_family', 'accept_credit_card',
# 'parking', 'outdoor_seating', 'wifi', 'wheelchair_accessible', 'price1', 'price2', 'cdate_restaurant_month', 'cdate_restaurant_day', 'cdate_restaurant_year']]


y_pred = clf.predict(X_test)

# y_pred = clf.predict(X_test_submit)


# print(y_pred)

print("Accuracy:", metrics.accuracy_score(y_test, y_pred))
# metrics.plot_roc_curve(clf, X_test, Y_test)
