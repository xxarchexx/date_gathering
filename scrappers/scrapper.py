import logging
import requests

class Scrapper:    	    
		def __init__(self, pageCounts):
			self.skip_objects = skip_objects
			self.pageNums = None  
			self.pageCounts = 100
			
			if pageCounts != None:
				self.pageCounts = pageCounts
			
			self.lastRequestID = 0
			self.last_status_code = 200
			self.usersUrl = 'https://fapi.stackexchange.com//2.2/users?page={0}&pagesize=100&fromdate=1483228800&todate=1514764800&order=desc&sort=reputation&site=stackoverflow'


		def scrap_process(self):
			self.GetLastUserRequestID(self)
			self.GetExistingPageNums(self)		
			i = 1    
			
			while i <= self.pageCounts :    		
				self.ImportRemoteDataByPage(self, i,conn)
				if LoaderFromApi.last_status_code == 200:
					i+=1
					#request delay 
					time.sleep(10)
		
  
		
		def GetLastUserRequestID(self):
			#Get Last ID from REQUESTS_USERS table
			conn = sqlite3.connect('StackData.db')    
			c = conn.cursor()
			c.execute("Select MAX(ID) from REQUESTS_USERS")
			all_rows = c.fetchone()
			conn.close()		
			row = all_rows[0]
			if row is  None:
				return		
				self.lastRequestID =   all_rows[0]
		
		
		def GetExistingPageNums(self):        
				"""Get  page numbers for determine  original request already had """
				conn = sqlite3.connect('StackData.db') 
				c = conn.cursor()
				c.execute("Select page_num from REQUESTS_USERS order by id")
				all_rows = c.fetchall()
				con.close()
				if all_rows is  None:
					return    
				self.pageNums =   all_rows  

		def ImportRemoteDataByPage(self,page_number,conn):    
			if self.requestusers is not None:    
				if page_number in self.pageNums:  
					return

			usersUrl = self.usersUrl.format(str(page_number))    
			res = requests.get(usersUrl)   
			self.last_status_code = res.status_code
		
			
			if res.status_code != 200:
				return	
	
			sqlInsertText = """ insert into REQUESTS_USERS (ID,PAGE_NUM,TEXT) VALUES (?,?,?) """  
			jsonstring = json.dumps(res.json())   
			c = conn.cursor()
			id = self.lastRequestID +1
			c.execute(sqlInsertText,(id,i,jsonstring))
			conn.commit()
			conn.close()
			LoaderFromApi.lastRequestID = id
	
   
