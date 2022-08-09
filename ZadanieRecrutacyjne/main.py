import csv
import ftplib
import os
import pymysql
import pandas as pd

# Step 1. Download file with date from ftp server to directory 'Data'
from archive import Archive

ftp = ftplib.FTP("138.201.56.185")
ftp.login("rekrut", "zI4wG9yM5krQ3d")
remotefile = 'task.rar'
pathToDownload = os.path.dirname(os.path.abspath(__file__)) + '\Data\\task.rar'
with open(pathToDownload,'wb') as file:
    ftp.retrbinary('RETR %s' % remotefile, file.write)

DataFilePath = os.path.dirname(os.path.abspath(__file__)) + '\Data\\'

# Step 2. Unzipping rar file
Archive(pathToDownload).extractall(DataFilePath)

# Step 3. Connection to local Database
mydb = pymysql.connect(host='localhost', port=3306, user='root', password='root', database="dataThings")
curs = mydb.cursor()

# if you not have, create Database
#curs.execute("CREATE DATABASE dataThings")

# Creat tables
curs.execute("CREATE TABLE data(Personid int NOT NULL AUTO_INCREMENT PRIMARY KEY,part_number VARCHAR(255), manufacturer VARCHAR(255), main_part_number VARCHAR(255), category VARCHAR(255), origin VARCHAR(2))")
curs.execute("CREATE TABLE deposit(Personid int NOT NULL AUTO_INCREMENT PRIMARY KEY,part_number VARCHAR(255),deposit FLOAT)")
curs.execute("CREATE TABLE price(Personid int NOT NULL AUTO_INCREMENT PRIMARY KEY,part_number VARCHAR(255),price DECIMAL(10,2))")
curs.execute("CREATE TABLE quantity(Personid int NOT NULL AUTO_INCREMENT PRIMARY KEY,part_number VARCHAR(255),quantity VARCHAR(3),warehouse VARCHAR(1))")
curs.execute("CREATE TABLE weight(Personid int NOT NULL AUTO_INCREMENT PRIMARY KEY,part_number VARCHAR(255), weight_unpacked VARCHAR(255), weight_packed VARCHAR(255))")

#curs.execute("DROP TABLE data")
#curs.execute("DROP TABLE deposit")
#curs.execute("DROP TABLE price")
#curs.execute("DROP TABLE quantity")
#curs.execute("DROP TABLE weight")


# load file data to Database
data = pd.read_csv(r'Data\data.csv', index_col=False, delimiter=';')
df = pd.DataFrame(data)
for row in df.itertuples():
    sql = ('''INSERT INTO data (part_number, manufacturer, main_part_number, category, origin)VALUES (%s,%s,%s,%s,%s)''')
    record = (row.part_number, row.manufacturer, row.main_part_number, row.category, row.origin)
    try:
        curs.execute(sql, record)
    except Exception as err:
        print("Duplicate record " + row.part_number)
mydb.commit()
print("file data successful reading")

# load file deposit to Database
data = pd.read_csv(r'Data\deposit.csv', index_col=False, delimiter=';')
df = pd.DataFrame(data)
for row in df.itertuples():
    sql = ('''INSERT INTO deposit (part_number, deposit)VALUES (%s,%s)''')
    record = (row.part_number, row.deposit.replace(',','.'))
    curs.execute(sql, record)
#    print(record)
mydb.commit()
print("file data deposit reading")

# load file price to Database
data = pd.read_csv(r'Data\price.csv', index_col=False, delimiter=';')
df = pd.DataFrame(data)
for row in df.itertuples():
    sql = ('''INSERT INTO price (part_number, price)VALUES (%s,%s)''')
    record = (row.part_number, row.price.replace(',','.'))
#    print(record)
    try:
        curs.execute(sql, record)
    except Exception as err:
        print("Duplicate record")
mydb.commit()
print("file data price reading")

# load file quantity to Database
data = pd.read_csv(r'Data\quantity.csv', index_col=False, delimiter=';')
df = pd.DataFrame(data)
for row in df.itertuples():
    sql = ('''INSERT INTO quantity (part_number, quantity,warehouse)VALUES (%s,%s,%s)''')
    record = (row.part_number, row.quantity.replace('>',''), row.warehouse)
#    print(record)
    try:
        curs.execute(sql, record)
    except Exception as err:
        print("Duplicate record")
mydb.commit()
print("file data quantity reading")

# load file wieght to Database
file1 = open('Data\weight.txt', 'r')
Lines = file1.readlines()
for line in Lines[1:]:
    spl = line.split('\t')
    record = (spl[0], spl[1], spl[2][:-1])
    sql = ('''INSERT INTO weight (part_number, weight_unpacked, weight_packed)VALUES (%s,%s,%s)''')
#    print(record)
    curs.execute(sql, record)
mydb.commit()
print("file data weight reading")

#curs.execute("DROP TABLE data")
#curs.execute("DROP TABLE deposit")
#curs.execute("DROP TABLE price")
#curs.execute("DROP TABLE quantity")
#curs.execute("DROP TABLE weight")


# TEST RECORDS
"""
curs.execute("SELECT COUNT(part_number) FROM data")
myresultData = curs.fetchall()
for x in myresultData:
    print(x)
curs.execute("SELECT COUNT(part_number) FROM deposit")
myresultDeposite = curs.fetchall()
for x in myresultDeposite:
    print(x)
curs.execute("SELECT COUNT(part_number) FROM Price")
myresultPrice = curs.fetchall()
for x in myresultPrice:
    print(x)
curs.execute("SELECT COUNT(part_number) FROM quantity")
myresultquantity = curs.fetchall()
for x in myresultquantity:
    print(x)
curs.execute("SELECT COUNT(part_number) FROM weight")
myresultWeight = curs.fetchall()
for x in myresultWeight:
    print(x)
"""
curs.execute("SELECT data.part_number, data.manufacturer, data.category, data.origin, price.price, deposit.deposit, cast(price.price+deposit.deposit as decimal(10,2)), quantity.quantity FROM data JOIN price ON data.part_number = price.part_number JOIN deposit ON data.part_number = deposit.part_number JOIN quantity ON data.part_number = quantity.part_number WHERE quantity.quantity NOT LIKE '0' AND price.price > 2.0 AND (quantity.warehouse LIKE 'A' OR quantity.warehouse LIKE 'H' OR quantity.warehouse LIKE 'J' OR quantity.warehouse LIKE '3' OR quantity.warehouse LIKE '9') ORDER BY quantity.warehouse asc ")
result = curs.fetchall()

# Save results to csv file
fp = open('Data\\result.csv', 'w', encoding="utf-8")
myFile = csv.writer(fp)
for row in result:
    myFile.writerow(row)
fp.close()

# Send result file
file = open('Data\\result.csv', 'rb')
#ftp.storbinary("STOR  complete/result by Dmytro.csv", file)
file.close()
ftp.quit()
curs.close()
mydb.close()
