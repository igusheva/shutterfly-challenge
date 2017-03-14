from collections import namedtuple
from datetime import datetime

Order = namedtuple('Order', 'event_time customer_id total_amount')

Image = namedtuple('Image','event_time customer_id camera_make camera_model')

Site = namedtuple('Site', 'event_time customer_id tags')

Customer = namedtuple('Customer','last_name adr_city adr_state total_expend total_visits max_date min_date exp_per_visit visit_per_week' )

class DBInterface(object):
    def getCustomers(self):
        raise NotImplementedError('')
###### CUSTOMER Start ########      
    def getCustomerById(self, key):
        raise NotImplementedError('')
    def addCustomer(self, key, customer):
        raise NotImplementedError('')
###### CUSTOMER End ########  
###### ORDER Start ########      
    def getOrderById(self, key):
        raise NotImplementedError('')
    def addOrder(self, key, order):
        raise NotImplementedError('')
###### ORDER END ########
###### IMAGE Start ########      
    def getImageById(self, key):
        raise NotImplementedError('')
    def addImage(self, key, image):
        raise NotImplementedError('')
###### IMAGE END ########  
###### SITE Start ########  
    def getSiteById(self, key):
        raise NotImplementedError('')
    def addSite(self, key, site):
        raise NotImplementedError('')
###### SITE END ######## 




class InMemoryDB(DBInterface):
    def __init__(self):
        self.customers={}
        self.orders={}
        self.images={}
        self.sites={}
    def getCustomers(self):
        return self.customers.keys()
###### CUSTOMER Start ########      
    def getCustomerById(self, key):
        return self.customers[key]        
    def addCustomer(self, key, customer):
        self.customers[key] = customer
###### CUSTOMER End ######## 
###### ORDER Start ########      
    def getOrderById(self, key):
        return self.orders[key]
    def addOrder(self, key,order):
        self.orders[key] = order      
###### ORDER END ########
###### IMAGE Start ########      
    def getImageById(self, key):
        return self.images[key]
    def addImage(self, key, image):
        self.images[key] = image        
###### IMAGE END ######## 
###### SITE Start ########  
    def getSiteById(self, key):
        return self.sites[key]
    def addSite(self, key, site):
        self.sites[key] = site
###### SITE END ######## 

    
    
        

def Ingest(event,DB):
    if event['key'] and event['type']:
###### CUSTOMER Start ########            
        if event['type'] == 'CUSTOMER':  
            if event['verb'] not in  ['NEW','UPDATE']:
                raise ValueError('Unknown action to take')  
            if event['verb'] == 'NEW':
                DB.addCustomer(event['key'], 
                           Customer(
                                last_name = event['last_name'], 
                                adr_city = event['adr_city'], 
                                adr_state = event['adr_state'],
                                total_expend = 0.0,
                                total_visits = 0,
                                max_date = None,
                                min_date = None,
                                exp_per_visit = None,
                                visit_per_week = None,
                                )
                              )
            else:
                oldCustomer = DB.getCustomerById(event['key'])
                DB.addCustomer(event['key'], 
                           Customer(
                                last_name = event['last_name'], 
                                adr_city = event['adr_city'], 
                                adr_state = event['adr_state'],
                                total_expend = oldCustomer.total_expend,
                                total_visits = oldCustomer.total_visits,
                                max_date = oldCustomer.max_date,
                                min_date = oldCustomer.min_date,
                                exp_per_visit = oldCustomer.exp_per_visit,
                                visit_per_week = oldCustomer.visit_per_week
                                )
                              )

###### CUSTOMER End ########      
###### ORDER Start ########      
        elif event['type'] == 'ORDER':
            if event['verb'] not in  ['NEW','UPDATE']:
                raise ValueError('Unknown action to take') 
            event_date = datetime.strptime(event['event_time'], "%Y-%m-%dT%H:%M:%S.%fZ")
            order_amount = float(event['total_amount'].split()[0])
            DB.addOrder(event['key'],
                        Order(
                                event_time = event_date, 
                                customer_id = event['customer_id'], 
                                total_amount = order_amount,
                                
                            )
                        )
            oldCustomer = DB.getCustomerById(event['customer_id'])
            total_expend = oldCustomer.total_expend + order_amount
            total_visits = oldCustomer.total_visits + 1
            min_date = event_date if oldCustomer.min_date is None else min(event_date, oldCustomer.min_date)
            max_date = event_date if oldCustomer.max_date is None else max(event_date, oldCustomer.max_date)
            exp_per_visit = total_expend / total_visits
            total_weeks = (max_date - min_date).total_seconds() / 60. / 60. / 24. / 7.
            visit_per_week = total_visits / total_weeks if total_weeks > 0. else None
            customer = Customer(
                            last_name = oldCustomer.last_name, 
                            adr_city = oldCustomer.adr_city, 
                            adr_state = oldCustomer.adr_state,
                            total_expend = total_expend,
                            total_visits = total_visits,
                            max_date = max_date,
                            min_date = min_date,
                            exp_per_visit = exp_per_visit,
                            visit_per_week = visit_per_week
                            
                        )
            DB.addCustomer(event['customer_id'],customer)
                
            
            
###### ORDER END ########    
###### IMAGE Start ########      
        elif event['type'] == 'IMAGE':
            if event['verb'] not in  ['UPLOAD']:
                raise ValueError('Unknown action to take')  
            DB.addImage(event['key'],
                        Image(
                                event_time = event['event_time'], 
                                customer_id = event['customer_id'], 
                                camera_make = event['camera_make'],
                                camera_model = event['camera_model']
                            )
                       )
###### IMAGE END ########  
###### SITE Start ########  
        elif event['type'] == 'SITE_VISIT':
            if event['verb'] not in  ['NEW']:
                raise ValueError('Unknown action to take')
            DB.addSite(event['key'],
                       Site(
                               event_time = event['event_time'], 
                               customer_id = event['customer_id'], 
                               tags = event['tags']
                            )
                      )
###### SITE END ########                  
    else:
        raise ValueError('No key value')
        
def TopXSimpleLTVCustomers(x, DB):
    for customer_id in DB.getCustomers():
        customer = DB.getCustomerById(customer_id)
        if customer.visit_per_week is not None:
            lvt = customer.exp_per_visit * customer.visit_per_week * 52 * 10
            print lvt
        

    

input = [{"type": "CUSTOMER", "verb": "NEW", "key": "96f55c7d8f42", "event_time": "2017-01-06T12:46:46.384Z", "last_name": "Smith", "adr_city": "Middletown", "adr_state": "AK"},
{"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e815502f", "event_time": "2017-01-06T12:45:52.041Z", "customer_id": "96f55c7d8f42", "tags": {"some key": "some value"}},
{"type": "IMAGE", "verb": "UPLOAD", "key": "d8ede43b1d9f", "event_time": "2017-01-06T12:47:12.344Z", "customer_id": "96f55c7d8f42", "camera_make": "Canon", "camera_model": "EOS 80D"},
{"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a43", "event_time": "2017-01-06T12:55:55.555Z", "customer_id": "96f55c7d8f42", "total_amount": "12.34 USD"},
{"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a63", "event_time": "2017-02-06T12:55:55.555Z", "customer_id": "96f55c7d8f42", "total_amount": "25.03 USD"}]

def main():
    DB = InMemoryDB()
    for e in input:
        Ingest(e, DB)
    customer = DB.getCustomerById('96f55c7d8f42')
    print customer
    TopXSimpleLTVCustomers(10, DB)
        
        
if __name__ == '__main__':
    main()
            
            
       
           
            






