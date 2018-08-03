import sqlite3
import pandas as pd

class Store(object):	
        def __init__(self):
            self.users = None      
            self.append = False		      
            self.df = None
            pd.set_option('display.height', 1000)
            pd.set_option('display.max_rows', 500)
            pd.set_option('display.max_columns', 500)
            pd.set_option('display.width', 1000)
        
        def Stats_data(self):
            conn = sqlite3.connect('StackData.db')   
            c = conn.cursor()
            self.df = pd.read_sql_query("select * from users;", conn)
            t = self.df.describe()            
            print(t.count)
            print(self.df.hist('bronze'))
           
            # self.getUsers(self)
        def read_data(self):
            conn = sqlite3.connect('StackData.db')   
            c = conn.cursor()
            self.df = pd.read_sql_query("select * from users;", conn)
            t = self.df.describe()
            
            print(t.count)
            print(self.df)
            x=1
            # df["user_id"].max()
            # self.getUsers(self)
        
        def write_data(self):
            append = False
            self.FillAllUsers(self,append)

        def append_data(self):
            append = True
            self.FillAllUsers(self,append)
            
            


        def getUsers(self,pagenumber):
            conn = sqlite3.connect('StackData.db')   
            c = conn.cursor()
                        
            if pagenumber != None:
                sqlexecute = """Select * from USERS where page_number = ?  """
                c.execute(sqlexecute,(pagenumber,))
            else:
                sqlexecute = """Select * from USERS """
                c.execute(sqlexecute)

            self.users =  c.fetchall()
            conn.close()  
            
        def FillAllUsers(self):
            self.users = None
            conn = sqlite3.connect('StackData.db')   
            c = conn.cursor()
            c.execute("select PAGE_NUM from REQUESTS_USERS") 
            pages = c.fetchall()       
            conn.close()
            sys.stdout.writelines(str(pages))
            for page in pages:
                time.sleep(5)          
                self.ParseUsersByPage(page[0]);
        
        def ParseUsersByPage(self,pageNumber):
            conn = sqlite3.connect('StackData.db')   
            c = conn.cursor()
            sqlSelect = """ select * from  REQUESTS_USERS where PAGE_NUM = ? """  
            c.execute(sqlSelect,(pageNumber,))
            returndata = c.fetchall()      
            conn.close()
            
            #fill global users by page number
            self.getUsers(self,pageNumber)               

            for row in returndata:
                #row[3] - json data
                self.ProcessData(self, row[3], pageNumber) 
        
        def ProcessData(self, json, pageNumber):
            items = json.loads(json)['items']
            self.ProcessUsers(items,pageNumber)	
                
        def ProcessUsers(self,items,pagenumber):      
        
            sqlexecute = """Insert Into USERS 
            (            
            account_id        
            ,bronze
            ,gold
            ,silver
            ,creation_date
            ,display_name
            ,is_employee
            ,last_access_date
            ,last_modified_date
            ,user_type
            ,user_id
            ,accept_rate
            ,location,
            page_number,
            reputation
            )
            values
            (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )"""
            
            
            if not append:
                conn = sqlite3.connect('StackData.db')   
                c = conn.cursor()
                deletesql = "Delete from users where page_number = ?"
                c.execute(deletesql,(pageNumber,))
                conn.commit()
                conn.close()			 
                self.users = None
            
            for item in items:
                conn = sqlite3.connect('StackData.db')   
                c = conn.cursor()
                
                if self.users != None:
                    if item['account_id'] in self.users:
                        continue                
                
                badges = item['badge_counts']
                
                accept_rate = ""

                if 'accept_rate' in  item.keys():
                    accept_rate = str(item['accept_rate'])
            
                location = ""
                if 'location' in  item.keys():
                    location = str(item['location'])
                
                last_modified_date = ""
                if 'last_modified_date' in  item.keys():
                    last_modified_date = str(item['last_modified_date'])
                if 'reputation' in  item.keys():
                    reputation = str(item['reputation'])
                    
                c.execute(sqlexecute,(
                item['account_id']            
                ,badges['bronze']
                ,badges['gold']
                ,badges['silver']
                ,item['creation_date']
                ,item['display_name']
                ,item['is_employee']
                ,item['last_access_date']
                ,last_modified_date
                ,item['user_type']
                ,item['user_id']  
                ,accept_rate
                ,location  
                ,pagenumber
                ,reputation
                ))
                conn.commit()
                conn.close 
        