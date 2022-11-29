from email.policy import default
from importlib.resources import contents
import pandas as pd
import threading
import time
import  datetime
start_time = time.perf_counter()
# longsent=0
# bol=[]
# bol2=[]
i=0  
s3_i=0
s3=0
with open('zaman.csv', mode='w',encoding="utf8") as file:
    file.write("")
data=pd.read_csv("deneme_rows.csv",low_memory=False)
df=data.loc[:,["Product","Issue","Company","State","Complaint ID","ZIP code"]].dropna()#istenilen sütun getirilir null değer içerenler silinir
content=[] 

def printer(k,value,value2,print_content,similar):
    liste_str=""
    liste2_str=""
    for i in range(len(print_content)):
        try:
            liste_str+=(value[int(print_content[i])])
            liste2_str+=(value2[int(print_content[i])])
        except TypeError:
            liste_str+=str(value[int(print_content[i])])+" "
            liste2_str+=str(value2[int(print_content[i])])+" "        
    print(k,liste_str,liste2_str,"-->",similar)            

    return content    

        
#*  
# def threadsayısı(t):
#     while(len(df)%3!=0):
#         n=len(df)/3
#         len)len-
#         hesapla(bas,son)

def threadSay():
    #print(t)
    global diziBas
    global diziSon
    diziBas=[]
    diziSon=[]
    son=int(0)
    artis=int(0)
    basl=int(1+artis)
    diziBas.append(basl)
    artis=round(len(df)/t)
    son=int(basl+artis-1)
    diziSon.append(son)
    while(son<len(df)):
        basl=int(son+1)
        son=int(basl+artis-1)
        if(basl!=len(df)):
            diziBas.append(basl)
        if(son>len(df)):
            son=len(df)
            diziSon.append(son)
            # if(son!=len(df)):
            #     diziSon.append(son)
        else:
            diziSon.append(son) 
    threadBasla(t)
    
#     print(diziBas)
#     print(diziSon)
#     print(artis)
class myThread (threading.Thread):
   def init(self,name):
      threading.Thread._init_(self)
      self.name = name
      
   def run(self):
      global anBas
      anBas=datetime.datetime.now()
    #   tarih1 = datetime.datetime.strftime(anBas, '%X')
      print ("Starting " + self.name)
      threadli(self.name,1,senaryo_no)
      print ("Exiting " + self.name)

def threadZaman(k,anBas,anSon):
    tarih1 = datetime.datetime.strftime(anBas, '%X')
    tarih2 = datetime.datetime.strftime(anSon, '%X')
    anDizi=anBas.strftime('%H:%M:%S').split(":")
    f1=datetime.timedelta(seconds=int(anDizi[2]))
    f2=datetime.timedelta(minutes=int(anDizi[1]))
    f3=datetime.timedelta(hours=int(anDizi[0]))
    anFark=anSon-f1-f2-f3
    anFark=datetime.datetime.strftime(anFark, '%X')
    # with open('zaman.csv', mode='a',encoding="utf8") as file:
    #     file.write(k+":"+str(tarih2)+"-"+str(tarih1)+"="+anFark+"\n")

def threadli(k,s,senaryo_no):
    a=k[-1]
    a=int(a)
    match senaryo_no:
        case 1|4:
            hesapla(k,diziBas[a-1],diziSon[a-1],same_product="",s3_i=0,s3=0)
        case 2:
            hesapla(k,diziBas[a-1],diziSon[a-1],same_product=same_product,s3_i=0,s3=0)
        case 3:
            hesapla(k,diziBas[a-1],diziSon[a-1],same_product="",s3_i=int(bul),s3=1)
        # case 4:
        #     hesapla(k,diziBas[a-1],diziSon[a-1],same_product="",s3_i=0,s3=0)
        case _:
            default
    
    print("Thread",k, "başladı")
    time.sleep(s)
    #print("delay",s)
    
def threadBasla(sayi):
    k=0
    threadList=[]
    while(k<sayi):
        threadList.append(k+1)
        k=k+1
    # print(threadList)
    bas=datetime.datetime.now()
    startAll=datetime.datetime.strftime(bas, '%X')
    basDizi=bas.strftime('%H:%M:%S').split(":")
    i=0
    threads = []
    for i in threadList:
       
        thread = myThread(args=(i))
        thread.start()
        threads.append(thread)
        i=i+1 
    for t in threads:
        t.join()
    son=datetime.datetime.now()    
    end=datetime.datetime.strftime(son, '%X')
    f1=datetime.timedelta(seconds=int(basDizi[2]))
    f2=datetime.timedelta(minutes=int(basDizi[1]))
    f3=datetime.timedelta(hours=int(basDizi[0]))
    fark=son-f1-f2-f3
    fark=datetime.datetime.strftime(fark, '%X')
    # fark=datetime.datetime.strftime(fark, '%X')
    print ("Exiting Main Thread")
    print(end,"-",startAll,"=",fark)
    # for i in range(sayi):
    #     print("Thread",i+1,"bas",bas[i],"son",son[i])
    #     thread_instance = threading.Thread(target=threadli, args=(i,2))
    #     thread_instance.start()
    #     threads.append(thread_instance)
    #     thread_instance.join()

def hesapla(k,bas,son,same_product,s3_i,s3):   
    global tarih2
    count=0
    liste=""#istenilen sütundaki cümlelerin liste hali
    liste2=""
    liste_str=""#cümlelerin string hali tutulur
    liste2_str=""  
    for i in range(bas,son+1):#dosya başından       
        # print(bas,"-",son+2)
        if(s3==1):
            n_range=range(bas,son+1)
        else:
            n_range= range(i+1,len(df.loc[:]))
        for j in n_range :#sonraki elemandna itibaren sona
            if(s3==1):
                value=df.loc[s3_i]
            else:
                value=df.loc[i]#ana satır
            try :
                value2=df.loc[j]#karşılaştırılacak satır
            except KeyError:
                break

            if(same_product!=""):
                if(value[same_product]!=value2[same_product]):
                    continue

            for v in content:#istenilen sütunların bilgisi alınır
                try:
                    liste_str+=(value[int(v)])
                    liste2_str+=(value2[int(v)])
                except TypeError:#complaint ID hatası için eklendi
                    liste_str+=str(value[int(v)])+" "
                    liste2_str+=str(value2[int(v)])+" " 
                
            liste=liste_str.split()#küçük harf üzerinden karşılaştırma
            liste2=liste2_str.split()
            len_1=len(liste)
            len_2=len(liste2)
            if(len_1>=len_2):
                for a in range(len_1):
                    if(liste[a] in liste2):
                        count+=1
                if(count!=0):            
                    similar=count/len_1*100
                    if(similar>=float(similar_no)):
                        printer(k,value=value,value2=value2,print_content=print_content,similar=similar)
                count=0           
            else:
                for b in range(len_2):
                    if(liste2[b] in liste):
                        count+=1
                if(count!=0):        
                    similar=count/len_2*100
                    if(similar>=float(similar_no)):
                        printer(k,value=value,value2=value2,print_content=print_content,similar=similar)
                count=0
            liste=""
            liste2=""
            liste_str=""
            liste2_str=""
        if(s3==1):
            break 
    anSon=datetime.datetime.now()
    tarih2 = datetime.datetime.strftime(anSon, '%X')
    threadZaman(k,anBas,anSon)

senaryo_no=1
#---------
sayi=input("benzerlik için seçilecek sütun sayısı: ")
while(sayi):
        sutun=input("sütun no: ")
        content+=sutun
        sayi-=1
print_content=[]
#---------       
print_no=input("çıktıda gösterilecek sütun sayısı: ")
print_no=int(print_no)
while(print_no):
    print_sutun=input("çıktı sütun no: ")
    print_content+=print_sutun
    print_no-=1 
#---------    
similar_no=input("min benzerlik oranı: ")
#---------
t=int(input("kullanılacak thrad sayısı: "))
same_product=""
same_product=int(input("aynı olacak sütun seçiniz: "))     
complaint_id=input("Complaint ID: ")
bul=df.loc[df["Complaint ID"] == int(complaint_id)]#3237250
bul=bul.index.values