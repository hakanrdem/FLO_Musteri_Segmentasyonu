###############################################################
# RFM Analizi İle FLO Müşteri Segmentasyonu (Customer Segmentation with RFM)
##############################################################

# 1. İş Problemi (Business Problem)
# 2. Veriyi Anlama (Data Understanding)
# 3. Veri Hazırlama (Data Preparation)
# 4. RFM Metriklerinin Hesaplanması (Calculating RFM Metrics)
# 5. RFM Skorlarının Hesaplanması (Calculating RFM Scores)
# 6. RFM Segmentlerinin Oluşturulması ve Analiz Edilmesi (Creating & Analysing RFM Segments)
# 7. Tüm Sürecin Fonksiyonlaştırılması

# 1. İş Problemi (Business Problem)

# Online ayakkabı mağazası olan FLO müşterilerini segmentlere ayırıp bu segmentlere göre pazarlama stratejileri belirlemek istiyor.
# Buna yönelik olarak müşterilerin davranışları tanımlanacak ve bu davranışlardaki öbeklenmelere göre gruplar oluşturulacak.

# Veri Seti Hikayesi
# Veri seti Flo’dan son alışverişlerini 2020 - 2021 yıllarında OmniChannel (hem online hem offline alışveriş yapan)
# olarak yapan müşterilerin geçmiş alışveriş davranışlarından elde edilen bilgilerden oluşmaktadır.

# Değişkenler

# master_id : Eşsiz müşteri numarası
# order_channel : Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile)
# last_order_channel : En son alışverişin yapıldığı kanal
# first_order_date : Müşterinin yaptığı ilk alışveriş tarihi
# last_order_date : Müşterinin yaptığı son alışveriş tarihi
# last_order_date_online : Müşterinin online platformda yaptığı son alışveriş tarihi
# last_order_date_offline : Müşterinin offline platformda yaptığı son alışveriş tarihi
# order_num_total_ever_online : Müşterinin online platformda yaptığı toplam alışveriş sayısı
# order_num_total_ever_offline : Müşterinin offline'da yaptığı toplam alışveriş sayısı
# customer_value_total_ever_offline : Müşterinin offline alışverişlerinde ödediği toplam ücret
# customer_value_total_ever_online : Müşterinin online alışverişlerinde ödediği toplam ücret
# interested_in_categories_12 : Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi

# Proje Görevleri

# Görev 1
# Veriyi Anlama ve Hazırlama

# Adım 1
# flo_data_20K.csvcverisinicokuyunuz.Dataframe’inckopyasını oluşturunuz.
import datetime as dt
import pandas as pd
pd.set_option("display.max_columns", None)
# pd.set_option("display.max_rows", None)
pd.set_option('display.width', 500)
pd.set_option("display.float_format", lambda x: "%.3f" % x)
df_ = pd.read_csv("/Users/hakanerdem/PycharmProjects/pythonProject/dsmlbc_9_abdulkadir/Homeworks/hakan_erdem/2_CRM_Analitigi/flo_data_20k.csv")
df = df_.copy()
df.head()

# Adım 2
# Verisetinde
# a. İlk 10 gözlem,
# b. Değişken isimleri,

df.columns

# c. Betimsel istatistik,
# d. Boş değer,
# e. Değişken tipleri, incelemesi yapınız.

def check_df(dataframe, head=10):
    print("##################### Shape #####################")
    print(dataframe.shape)
    print("##################### Types #####################")
    print(dataframe.dtypes)
    print("##################### Head #####################")
    print(dataframe.head(head))
    print("##################### Tail #####################")
    print(dataframe.tail(head))
    print("##################### NA #####################")
    print(dataframe.isnull().sum())
    print("##################### Quantiles #####################")
    print(dataframe.describe([0, 0.05, 0.50, 0.95, 0.99, 1]).T)

check_df(df)

# Adım 3
# Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir.
# Her bir müşterinin toplam alışveriş sayısı ve harcaması için yeni değişkenler oluşturunuz.

# ! #
# order_num_total_ever_online : Müşterinin online platformda yaptığı toplam alışveriş sayısı
# order_num_total_ever_offline : Müşterinin offline'da yaptığı toplam alışveriş sayısı
# customer_value_total_ever_offline : Müşterinin offline alışverişlerinde ödediği toplam ücret
# customer_value_total_ever_online : Müşterinin online alışverişlerinde ödediği toplam ücret

# Her bir müşterinin toplam alışveriş sayısı
df["omnichannel_order_num_total"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]

# Her bir müşterinin toplam harcaması
df["omnichannel_customer_value_total"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]
df.head()

# Adım 4
# Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
df.columns
df.dtypes

# Tarih ifade eden değişkenler
# first_order_date : Müşterinin yaptığı ilk alışveriş tarihi
# last_order_date : Müşterinin yaptığı son alışveriş tarihi
# last_order_date_online : Müşterinin online platformda yaptığı son alışveriş tarihi
# last_order_date_offline : Müşterinin offline platformda yaptığı son alışveriş tarihi

df['first_order_date'] = pd.to_datetime(df['first_order_date'])
df['last_order_date'] = pd.to_datetime(df['last_order_date'])
df['last_order_date_online'] = pd.to_datetime(df['last_order_date_online'])
df['last_order_date_offline'] = pd.to_datetime(df['last_order_date_offline'])
df.dtypes

# Adım 5
# Alışveriş kanallarındaki müşteri sayısının,
# toplam alınan ürün sayısının ve toplam harcamaların dağılımına bakınız.

df["master_id"].nunique()
df.groupby('last_order_channel').agg({
                                        "master_id": lambda master_id: master_id.nunique(),
                                        "omnichannel_order_num_total": lambda omnichannel_order_num_total: omnichannel_order_num_total.sum(),
                                        "omnichannel_customer_value_total": lambda omnichannel_customer_value_total: omnichannel_customer_value_total.sum()})

# Adım 6
# En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.

#new_df = df[["master_id","omnichannel_customer_value_total"]]
#new_df = new_df.sort_values(by= ["omnichannel_customer_value_total"], ascending=False)
#new_df.head(10)

df.groupby("master_id").agg({"omnichannel_customer_value_total": "sum"}).sort_values("omnichannel_customer_value_total", ascending=False).head(10)

# Adım 7
# En fazla siparişi veren ilk 10 müşteriyi sıralayınız.

#new_df = df[["master_id","omnichannel_order_num_total"]]
#new_df = new_df.sort_values(by= ["omnichannel_order_num_total"], ascending=False)
#new_df.head(10)

df.groupby("master_id").agg({"omnichannel_order_num_total": "sum"}).sort_values("omnichannel_order_num_total", ascending=False).head(10)

# Adım 8
# Veri ön hazırlık sürecini fonksiyonlaştırınız.

def create_cd(dataframe):

    dataframe["omnichannel_order_num_total"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["omnichannel_customer_value_total"] = dataframe["customer_value_total_ever_offline"] + dataframe["customer_value_total_ever_online"]
    dataframe['first_order_date'] = pd.to_datetime(df['first_order_date'])
    dataframe['last_order_date'] = pd.to_datetime(df['last_order_date'])
    dataframe['last_order_date_online'] = pd.to_datetime(df['last_order_date_online'])
    dataframe['last_order_date_offline'] = pd.to_datetime(df['last_order_date_offline'])

create_cd(df)
df.head()
df.dtypes

# Görev 2
# RFM Metriklerinin Hesaplanması

# Adım 1
# Recency, Frequency ve Monetary tanımlarını yapınız.

# RFM = Recency, Frequency, Monetary RFM metrikleridir.
# Bu metrikler kullanılarak belirli skorlar elde edilir ve bu skorlara göre müşteriler segmentlere ayırılır.
# Recency(yenilik) : Müşterinin en son ne zaman alışveriş yaptığını durumunu belirtmektedir.
# Hesaplanırken analizin yapıldığı tarihten müşterinin son yaptığı alışveriş tarihi çıkarılır.
# Frequency(Sıklık) : Müşterinin yaptığı alışveriş sayısıdır.İşlem sayısı sıklığıdır.
# Monetary(Parasal Değer)  : Müşterilerin bıraktığı parasal değerdir.

# Adım 2
# Müşteri özelinde Recency, Frequency ve Monetary metriklerini hesaplayınız.

# Recency, Frequency, Monetary
df["last_order_date"].max()
# Timestamp('2021-05-30 00:00:00')  > Biz 2 gün sonrasını yani 2021-06-02 tarihini kontrol edeceğiz.

today_date = dt.datetime(2021, 6, 2)
type(today_date)


rfm = df.groupby('master_id').agg({'last_order_date': lambda last_order_date: (today_date - last_order_date.max()).days,
                                     'omnichannel_order_num_total': lambda omnichannel_order_num_total: omnichannel_order_num_total.sum(),
                                   'omnichannel_customer_value_total' : lambda omnichannel_customer_value_total: omnichannel_customer_value_total.sum()})

rfm.head()

# Adım 3
# Hesapladığınız metrikleri rfm isimli bir değişkene atayınız.
# Adım 4
# Oluşturduğunuz metriklerin isimlerini recency, frequency ve monetary olarak değiştiriniz.

rfm.columns = ['recency', 'frequency', 'monetary']

# Görev 3
# RF Skorunun Hesaplanması

# Adım 1
# Recency, Frequency ve Monetary metriklerini qcut yardımı ile 1-5 arasında skorlara çeviriniz.

# Adım 2
# Bu skorları recency_score, frequency_score ve monetary_score olarak kaydediniz.

rfm["recency_score"] = pd.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1])

rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])

rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5, labels=[1, 2, 3, 4, 5])

# Adım 3
# recency_score ve frequency_score’u tek bir değişken olarak ifade ediniz ve RF_SCORE olarak kaydediniz.

rfm["RFM_SCORE"] = rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str)

# Görev 4
# RF Skorunun Segment Olarak Tanımlanması

# Adım 1
# Oluşturulan RF skorları için segment tanımlamaları yapınız.

# RFM isimlendirmesi

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
    r'5[4-5]': 'champions'
}

# Adım 2
# Aşağıdaki seg_map yardımı ile skorları segmentlere çeviriniz.

rfm["segment"] = rfm["RFM_SCORE"].replace(seg_map, regex=True)
rfm.head()

# Görev 5
# Aksiyon Zamanı !

# Adım 1
# Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.

rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean","count"])

rfm[rfm["segment"] == "cant_loose"].head()
rfm[rfm["segment"] == "champions"].head()


# Adım 2
# RFM analizi yardımıyla aşağıda verilen 2 case için ilgili profildeki müşterileri bulun ve müşteri id'lerini csv olarak kaydediniz.

a)

new_df1 = rfm[(rfm["segment"] == "champions") | (rfm["segment"] == "loyal_customers")]
new_df2 = df[(df["interested_in_categories_12"]).str.contains("KADIN")]

end_df = pd.merge(new_df1,new_df2[["interested_in_categories_12","master_id"]],on=["master_id"])

end_df.head()
end_df.to_csv("loyal_customer_id_info.csv")

b)

new_df3 = rfm[(rfm["segment"]=="cant_loose") | (rfm["segment"]=="about_to_sleep") | (rfm["segment"]=="new_customers")]

new_df4 = df[(df["interested_in_categories_12"]).str.contains("ERKEK|COCUK")]

end_df2 = pd.merge(new_df3,new_df4[["interested_in_categories_12","master_id"]],on=["master_id"])

end_df2.to_csv("customer_id_info.csv")




