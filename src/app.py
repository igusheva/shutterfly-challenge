from collections import namedtuple
from datetime import datetime

SECONDS_PER_WEEK = 60. * 60. * 24. * 7.

Order = namedtuple('Order', 'event_time customer_id total_amount')

Image = namedtuple('Image','event_time customer_id camera_make camera_model')

Site = namedtuple('Site', 'event_time customer_id tags')

Customer = namedtuple('Customer','last_name adr_city adr_state total_expend total_visits max_date min_date exp_per_visit visit_per_week' )

class DBInterface(object):
    """Abstract Database interface.

    The interface contains only low-level access to data without any business
    logic.
    """
    def getCustomers(self):
        raise NotImplementedError('')
    def getCustomerById(self, key):
        raise NotImplementedError('')
    def addCustomer(self, key, customer):
        raise NotImplementedError('')

    def getOrderById(self, key):
        raise NotImplementedError('')
    def addOrder(self, key, order):
        raise NotImplementedError('')

    def getImageById(self, key):
        raise NotImplementedError('')
    def addImage(self, key, image):
        raise NotImplementedError('')

    def getSiteById(self, key):
        raise NotImplementedError('')
    def addSite(self, key, site):
        raise NotImplementedError('')


class InMemoryDB(DBInterface):
    """In memoery implementation of Database."""
    def __init__(self):
        self.customers={}
        self.orders={}
        self.images={}
        self.sites={}
    def getCustomers(self):
        return self.customers.keys()
    def getCustomerById(self, key):
        return self.customers[key]
    def addCustomer(self, key, customer):
        self.customers[key] = customer

    def getOrderById(self, key):
        return self.orders[key]
    def addOrder(self, key,order):
        self.orders[key] = order

    def getImageById(self, key):
        return self.images[key]
    def addImage(self, key, image):
        self.images[key] = image

    def getSiteById(self, key):
        return self.sites[key]
    def addSite(self, key, site):
        self.sites[key] = site


def Ingest(event,DB):
    if event['key'] and event['type']:
        if event['type'] == 'CUSTOMER':
            if event['verb'] not in  ['NEW','UPDATE']:
                raise ValueError('Unknown action to take')
            if event['verb'] == 'NEW':
                DB.addCustomer(
                    event['key'],
                    Customer(
                         last_name = event['last_name'],
                         adr_city = event['adr_city'],
                         adr_state = event['adr_state'],
                         total_expend = 0.0,
                         total_visits = 0,
                         max_date = None,
                         min_date = None,
                         exp_per_visit = None,
                         visit_per_week = None))
            else:
                oldCustomer = DB.getCustomerById(event['key'])
                DB.addCustomer(
                    event['key'],
                    Customer(
                         last_name = event['last_name'],
                         adr_city = event['adr_city'],
                         adr_state = event['adr_state'],
                         total_expend = oldCustomer.total_expend,
                         total_visits = oldCustomer.total_visits,
                         max_date = oldCustomer.max_date,
                         min_date = oldCustomer.min_date,
                         exp_per_visit = oldCustomer.exp_per_visit,
                         visit_per_week = oldCustomer.visit_per_week))

        elif event['type'] == 'ORDER':
            if event['verb'] not in  ['NEW','UPDATE']:
                raise ValueError('Unknown action to take')
            event_date = datetime.strptime(event['event_time'], "%Y-%m-%dT%H:%M:%S.%fZ")
            order_amount = float(event['total_amount'].split()[0])
            DB.addOrder(event['key'],
                        Order(event_time = event_date,
                              customer_id = event['customer_id'],
                              total_amount = order_amount))
            oldCustomer = DB.getCustomerById(event['customer_id'])
            total_expend = oldCustomer.total_expend + order_amount
            total_visits = oldCustomer.total_visits + 1
            min_date = (event_date if oldCustomer.min_date is None
                        else min(event_date, oldCustomer.min_date))
            max_date = (event_date if oldCustomer.max_date is None else
                        max(event_date, oldCustomer.max_date))
            exp_per_visit = total_expend / total_visits
            total_weeks = ((max_date - min_date).total_seconds() /
                           SECONDS_PER_WEEK)
            visit_per_week = (total_visits / total_weeks if total_weeks > 0.
                              else None)
            customer = Customer(
                            last_name = oldCustomer.last_name,
                            adr_city = oldCustomer.adr_city,
                            adr_state = oldCustomer.adr_state,
                            total_expend = total_expend,
                            total_visits = total_visits,
                            max_date = max_date,
                            min_date = min_date,
                            exp_per_visit = exp_per_visit,
                            visit_per_week = visit_per_week)
            DB.addCustomer(event['customer_id'],customer)

        elif event['type'] == 'IMAGE':
            if event['verb'] not in  ['UPLOAD']:
                raise ValueError('Unknown action to take')
            DB.addImage(event['key'],
                        Image(
                                event_time = event['event_time'],
                                customer_id = event['customer_id'],
                                camera_make = event['camera_make'],
                                camera_model = event['camera_model']))

        elif event['type'] == 'SITE_VISIT':
            if event['verb'] not in  ['NEW']:
                raise ValueError('Unknown action to take')
            DB.addSite(event['key'],
                       Site(
                               event_time = event['event_time'],
                               customer_id = event['customer_id'],
                               tags = event['tags']))
    else:
        raise ValueError('No key value')


def TopXSimpleLTVCustomers(x, DB):
    customerList = []
    for customer_id in DB.getCustomers():
        customer = DB.getCustomerById(customer_id)
        if customer.visit_per_week is not None:
            lvt = customer.exp_per_visit * customer.visit_per_week * 52 * 10
            customerList.append((lvt,customer))
    customerLastNameList = []
    for _, customer in sorted(customerList,reverse=True)[0:x]:
        customerLastNameList.append(customer.last_name)
    return customerLastNameList
