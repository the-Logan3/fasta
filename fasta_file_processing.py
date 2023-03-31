#!/usr/bin/env python
# coding: utf-8

# In[ ]:


slno = 0

f = open("E:/Downloads/prot/fasta_data.csv","w")
f.write("SlNo,ProtID,Gene,ProtSeq,ProtLen")
f.close()

import pymysql as sql

user_name = 'root'
user_host = 'localhost'
user_port = 3306
user_database = '******'
user_password = '******'

connection = sql.connect(user = user_name, host = user_host, port = user_port, database = user_database, password = user_password)
db = connection.cursor()
db.execute("create table if not exists ProtData (SlNo int, ProtID varchar(20), Gene varchar(20), ProtSeq Text, ProtLen int)")
connection.commit()
db.close()
connection.close()
print ("Table created")

#connection = sql.connect(user = user_name, host = user_host, port = user_port, database = user_database, password = user_password)
#db = connection.cursor()
def loadIntoDB(data):
    connection = sql.connect(user = user_name, host = user_host, port = user_port, database = user_database, password = user_password)
    db = connection.cursor()
    print (data["SlNo"], data["ProtID"],data["Gene"], data["ProtSeq"],data["ProtLen"])
    db.execute("insert into ProtData (SlNo,ProtID,Gene,ProtSeq,ProtLen) values (%s,'%s','%s','%s',%s)"%(data["SlNo"], data["ProtID"],
                                                                                                 data["Gene"], data["ProtSeq"],
                                                                                                 data["ProtLen"]))
    connection.commit()
    db.close()
    connection.close()


def dataCreate(datacontent):
    global slno
    row = datacontent[0].strip().split("|")
    prot_id,gene = row[1],row[2].split("_HUMAN")[0]
    seq = ''.join([x.strip() for x in datacontent[1:]])
    #print (slno,prot_id,gene, seq, len(seq))
    data = {"SlNo":slno, "ProtID":prot_id,"Gene":gene, "ProtSeq":seq, "ProtLen":len(seq)}
    loadIntoDB(data)
    f = open("E:/Downloads/prot/fasta_data.csv","a")
    f.write("\n%s,%s,%s,%s,%s"%(slno,prot_id,gene,seq,len(seq)))
    f.close()

with open("E:/Downloads/prot/data.fasta", "r") as fl:
    content = fl.readlines()
    block_index = [x for x in range(len(content)) if ">" in content[x]]
    print (block_index)
    indx = 0
    slno = 1
    for i in block_index:
        row = content[i].strip().split("|")
        prot,gene = row[1],row[2].split("_")[0]
        
        if indx != len(block_index) - 1:
            #print (i, block_index[indx + 1])
            data_content = content[i:block_index[indx + 1]]
            #print (data_content)
            dataCreate(data_content)
            #print ("")
            slno += 1
        else:
            #print (i)
            data_content = content[i:]
            #print (data_content)
            dataCreate(data_content)
            slno += 1
        indx += 1

# connection.commit()
# db.close()
# connection.close()
print ("End of Program")
print ("File created and data loaded")
print ("DB created and data uploaded")

