##RFM


#master_id : Eşsiz müşteri numarası
#order_channel : Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile)
#last_order_channel : En son alışverişin yapıldığı kanal
#first_order_date : Müşterinin yaptığı ilk alışveriş tarihi
#last_order_date : Müşterinin yaptığı son alışveriş tarihi
#last_order_date_online : Müşterinin online platformda yaptığı son alışveriş tarihi
#last_order_date_offline : Müşterinin offline platformda yaptığı son alışveriş tarihi
#order_num_total_ever_online : Müşterinin online platformda yaptığı toplam alışveriş sayısı
#order_num_total_ever_offline : Müşterinin offline'da yaptığı toplam alışveriş sayısı
#customer_value_total_ever_offline : Müşterinin offline alışverişlerinde ödediği toplam ücret
#customer_value_total_ever_online : Müşterinin online alışverişlerinde ödediği toplam ücret
#interested_in_categories_12 : Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi


import datetime as dt
import pandas as pd
pd.set_option('display.max_columns',None) #-değişkenler
pd.set_option('display.max_rows', None) #-satırlar
pd.set_option('display.float_format', lambda x: '%.3f' % x)
df_ = pd.read_csv(r'C:\Users\elifd\PycharmProjects\pythonProject1\flo_data_20k.csv')

df= df_.copy()


###### Veriyi Anlama ve Hazırlama 

df.head(10)

df.columns

df.describe().T

df.isnull().sum()

df.info()

df.dtypes


df['total_order'] = df['order_num_total_ever_online'] + df['order_num_total_ever_offline']

df['total_value'] = df['customer_value_total_ever_offline'] + df['customer_value_total_ever_online']



# Tarihi ifade eden değişkenlerin tipini date'e çevirme

df.info()

date_columns = df.columns [df.columns.str.contains('date')]
df[date_columns] = df[date_columns].apply(pd.to_datetime)




# Alışveriş kanallarındaki müşteri sayısının, toplam alınan ürün sayısının ve toplam harcamaların dağılımına bakma

df.groupby('order_channel').agg({'master_id': 'count', 'total_order' : 'sum',
                                'total_value' : 'sum' })



#En fazla kazancı getiren ilk 10 müşteriyi sıralama

df.groupby('master_id').agg({'total_value' : 'sum'}).sort_values(by='total_value', ascending=False).head(10)



#En fazla siparişi veren ilk 10 müşteriyi sıralama

df.groupby('master_id').agg({'total_order' : 'sum'}).sort_values(by='total_order', ascending=False).head(10)




#fonksiyonlaştırma

def create_df(dataframe):
    df["total_order"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
    df["total_price"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_offline"]
    date_columns = df.columns[df.columns.str.contains("date")]
    df[date_columns] = df[date_columns].apply(pd.to_datetime)
    df.groupby("order_channel").agg({"master_id": "count", "total_order": ["sum", "mean"],
                                     "total_price": ["sum", "mean"]})
    df.sort_values(by="total_price", ascending=False).head(10)
    df.sort_values(by="total_order", ascending=False).head(10)
    return df

###### RFM Metriklerinin Hesaplanması ######
#Müşteri özelinde Recency, Frequency ve Monetary metriklerini hesaplama

df["last_order_date"].max()
df['last_order_date_online'].max()
df['last_order_date_offline'].max()

#analiz günü belirleme###

today_date = dt.datetime(2021, 6, 1)
type(today_date)

##or

t = pd.Timestamp('2021-06-01 00:00:00')
t + pd.DateOffset(days=2) == t

type(t)

today_date==t


 rfm= df.groupby('master_id').agg({'last_order_date': lambda last_order_date: (today_date - last_order_date.max()).days,
                                     'total_order': lambda x : x,
                                     'total_value': lambda x : x})
rfm.head()

rfm.describe().T

rfm.columns = ['recency', 'frequency', 'monetary']

rfm.head()


today_date = dt.datetime(2021, 6, 1)
type(today_date)



######## RF Skorunun Hesaplanması#######
#Recency, Frequency ve Monetary metriklerini qcut yardımı ile 1-5 arasında skorlara çevirme

rfm['recency_score'] = pd.qcut(rfm['recency'], 5, labels=[5,4,3,2,1])
rfm['frequency_score'] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels= [1,2,3,4,5])
rfm['monetary_score'] = pd.qcut(rfm['monetary'], 5, labels= [1, 2, 3, 4, 5])

rfm.head()

rfm.sort_values(by='master_id', ascending=False)

rfm["RF_SCORE"] = (rfm['recency_score'].astype(str) +
                    rfm['frequency_score'].astype(str))


#######RF Skorunun Segment Olarak Tanımlanması######
#Oluşturulan RF skorları için segment tanımlama


seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'}


#seg_map yardımı ile skorları segmentlere çevirme

rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)

####### Aksiyon Zamanı !
#Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleme

rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean", "count"])


#yeni bir kadın ayakkabı markası dahil ediyor.Dahil ettiği markanın ürün fiyatları genel müşteri tercihlerinin üstünde.
# Bu nedenle markanın tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak iletişime geçmek isteniliyor.
# Sadık müşterilerinden(champions,loyal_customers)ve kadın kategorisinden alışveriş yapan kişiler özel olarak iletişim kurulacak müşteriler.
# Bu müşterilerin id numaralarını csv dosyasına kaydetme


df_rfm = pd.merge(df, rfm, on='master_id')

df_rfm.head()

rfm_new = df_rfm[((df_rfm['segment'] == 'champions') | (df_rfm['segment'] == 'loyal_customer'))  & (df_rfm['interested_in_categories_12'].apply(lambda x: "KADIN" in x))]
rfm_new

#csv oluşturma

rfm_new.to_csv("master_id.csv")
rfm_new.to_csv("rfm_new.csv")


# Erkek ve Çocuk ürünlerinde %40'a yakın indirim planlanmaktadır.
# Bu indirimle ilgili kategorilerle ilgilenen geçmişte iyi müşteri olan ama uzun süredir alışveriş yapmayan kaybedilmemesi gereken müşteriler,
# uykuda olanlar ve yeni gelen müşteriler özel olarak hedef alınmak isteniyor.
# Uygun profildeki müşterilerin id'lerini csv dosyasına kaydetme



rfm_neww= df_rfm[((df_rfm['segment'] == 'hipernating') |
                      (df_rfm['segment'] == 'about_to_sleep')) &
                     (df_rfm['segment'] == 'new_customer') &
                     (df_rfm['interested_in_categories_12'].str.contains('ERKEK'))|
                     (df_rfm['interested_in_categories_12'].str.contains('COCUK'))]
rfm_neww

rfm_neww.to_csv("master_id.csv")
rfm_neww.to_csv("rfm_neww.csv")








