# Importing the libraries
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
import mysql.connector
from mysql.connector import Error
import datetime
import time
 

# K -means algorithm
def kmeans_algorithm(input_data):
    X = input_data.iloc[:,[0,2]].values
    
    from sklearn.cluster import KMeans
    wcss =[]
    for i in range (1,11):
        kmeans = KMeans(n_clusters = i, init = 'k-means++', max_iter =300, n_init = 10, random_state = 0)
        kmeans.fit(X)
        wcss.append(kmeans.inertia_)
    
    kmeans=KMeans(n_clusters= 3, init = 'k-means++', max_iter = 300, n_init = 10, random_state = 0)
    Y_Kmeans = kmeans.fit_predict(X)
    input_data['Cluster']=Y_Kmeans
    print('input_data: ')
    print(input_data)
    input_data.to_csv('kmeans_op.csv', index=False)
    return input_data

def convert_into_binary(x):
    if x == 'yes':
        return 1
    else:
        return 0

# Association algorithm
def apriori_algorithm(df):
    print('inside function')
    basket = pd.pivot_table(data=df,index='Tran_ID',columns='Site',values='Is_Traffic', \
                        aggfunc='sum',fill_value=0)
    basket_sets = basket.applymap(convert_into_binary)
    frequent_itemsets = apriori(basket_sets, min_support=0.003, use_colnames=True)
    #print(frequent_itemsets)
    rules_mlxtend = association_rules(frequent_itemsets, metric="lift", min_threshold=1)
    #print(rules_mlxtend)
    dt=rules_mlxtend[ (rules_mlxtend['lift'] >= 3) & (rules_mlxtend['confidence'] >= 0.5) ]
    dt["antecedents"] = dt['antecedents'].astype(str)
    dt["consequents"] = dt['consequents'].astype(str)
    dt["antecedents"] = dt['antecedents'].str.replace('frozenset','').str.replace('}','').str.replace('{','').str.replace('(','').str.replace(')','')
    dt["consequents"] = dt['consequents'].str.replace('frozenset','').str.replace('}','').str.replace('{','').str.replace('(','').str.replace(')','') 
    print(dt)
    dt.to_csv('apriori_.csv', index=False)
    df.drop('Cluster',axis=1,inplace=True)
    
    data= pd.DataFrame(columns=['Tran_ID', 'Region', 'Site','Detector','Sum_Volume','Avg_Volume','Is_Traffic'])
    dataf= pd.DataFrame(data)
    for i in df.index:
        string_1=str(df['Site'][i])
        
        if(dt["antecedents"].str.contains(string_1).any()):
            if(dataf.empty):
                dataf=df[df["Site"]== df['Site'][i]]
            else:
                dataf=pd.concat([df[df["Site"]== df['Site'][i]],dataf])          
    dataf = dataf.drop_duplicates()    
   
    
    return dataf 

# Database connection
def SQL_Connect():
    try:
        connection = mysql.connector.connect(host='centercloud.cxxqxdhi9kx1.eu-west-1.rds.amazonaws.com',
                                         database='centercloud',
                                         user='root',
                                         password='12345678')
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)        
            df1 = pd.read_sql('SELECT * FROM centercloud', con=connection)              
            

    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed")
    return df1

# Main fucntion
def main():
    
    ipstring = int(input("Is traffic on site: "))
    import os.path
    
    if(os.path.exists('OP.csv')):
        ipdf=pd.read_csv('OP.csv')
        print(ipdf)
        
        if(ipstring in ipdf['Site'].values):
            currentDT = datetime.datetime.now()
            print(str(currentDT))
            print('Yes')
            EndDT = datetime.datetime.now()
            totalDT=EndDT-currentDT
            print(totalDT.seconds)
        else:
            currentDT = datetime.datetime.now()
            time.sleep(10)
            print(str(currentDT))
            dataset=SQL_Connect()
            print(dataset)
            print('reading data done..')
            length=len(dataset.columns)
            
            flag=True
            count=0
            
            while count <= length:
                for column_headers in dataset.columns:
                    if dataset[column_headers].isnull().values.any():
                        dataset.drop([column_headers], axis=1, inplace=True)
                        #print (column_headers + 'dropped')
                        break
                
                if(flag==True):
                        break
                
                count=count+1
                        
            length=len(dataset.columns)
            print('reading data done..')
            print('start of kmeans..')
            kmeansDataset=kmeans_algorithm(dataset)
            print('end of kmeans..')
            print('start of apriori..')
            dt=apriori_algorithm(kmeansDataset)
            dt.to_csv('OP.csv', index=False)
            print('end of apriori..')
            EndDT = datetime.datetime.now()
            totalDT=EndDT-currentDT
            print('response time: '+str(totalDT.seconds.__round__()))
            print('no')
    
    else:
        time.sleep(5)
        currentDT = datetime.datetime.now()
        print(str(currentDT))
        dataset=SQL_Connect()
        print(dataset)
        print('reading data done..')
        length=len(dataset.columns)
        flag=True
        count=0
         
        while count <= length:
            for column_headers in dataset.columns:
                if dataset[column_headers].isnull().values.any():
                    dataset.drop([column_headers], axis=1, inplace=True)
                    #print (column_headers + 'dropped')
                    break
              
                if(flag==True):
                    break
                
                count=count+1
                        
            length=len(dataset.columns)
            print('reading data done..')
            print('start of kmeans..')
            kmeansDataset=kmeans_algorithm(dataset)
            print('end of kmeans..')
            print('start of apriori..')
            dt=apriori_algorithm(kmeansDataset)
            dt.to_csv('OP.csv', index=False)
            print('end of apriori..')
            EndDT = datetime.datetime.now()
            totalDT=EndDT-currentDT
            print('response time: '+str(totalDT.seconds))
            print('no')

    print('Done')

if __name__ == '__main__':
    main()



