import pymongo
from pprint import pprint
from bson import ObjectId
from pymongo import MongoClient
from datetime import date
import re

client = MongoClient('ec2-54-80-176-84.compute-1.amazonaws.com', 27017)
yelpDB =  client.yelpDB
business = yelpDB.business
reviews = yelpDB.reviews

#function1 find businesses by given zip code
def findByZip(zip):
   for x in business.find({"postal_code": zip}, {"name": 1, "business_id": 1, "postal_code": 1}).limit(10):
      pprint(x)
#findByZip("93101")

#function2 Find businesses in a given city
def findByCity(city):
   for x in business.find({"city": city}, {"name": 1, "business_id": 1, "city": 1}).limit(10):
      pprint(x)
#findByCity("Nashville")

#function3 find businesses by a given genre/category
def findByGenre(genre):
   for x in business.find({}, {"business_id": 1, "name": 1, "categories": 1}):
      if genre in x["categories"]:  
         pprint(x)
#findByGenre("Italian")

#function4 find businesses by a given name
def findByName(name):
   for x in business.find({"name": name}, {"name": 1, "business_id": 1, "address": 1}).limit(10):
      pprint(x)
#findByName("McDonald's")

#function5 find business by number of stars
def findByStars(stars):
   for x in business.find({"stars": {"$gte": stars}}, {"name": 1, "business_id": 1, "stars": 1}).sort("name").limit(10):
      pprint(x)
#findByStars(5)

#function6 find the address of a specific business
def findAddress(businessID):
   for x in business.find({"business_id": businessID}, {"name": 1, "address": 1, "business_id": 1}):
      print(x["name"] + " address: " + x["address"] + ",   id: " + x["business_id"])
#findAddress("CAIIPqKY0pS0yQ8fKGlDPg")

#function7 Make a new business
def makeBusiness(id, name, address, city, state, postal_code, lat, long, stars, categories, hours):
   busines = {"business_id": id, "name": name, "address": address, "city": city, "state": state, "postal_code": postal_code, "latitude": lat, "longitude": long, 
              "stars": stars, "review_count": 0, "categories": categories, "hours": hours}
   business.insert_one(busines)
   x = business.find_one({"business_id": id})
   pprint(x)
#makeBusiness("test", "TEST BUSINESS", "1 ROAD 1", "San Jose", "CA", "11111", "1", "2", "5", "testing, Italian", {"monday": "1:00-2:00"})
      
#function8 all of a certain restaurant's average rating in stars
def avgReviews(businessName):
   count = 0
   stars = 0
   for x in business.find({"name": businessName}, {"stars": 1}):
      count = count + 1
      stars = stars + x["stars"]
   avg = stars/count
   print(businessName + " average stars: " + str(avg))
#avgReviews("McDonald's")
   
#function9 find a business's hours
def businessHours(businessID):
   for x in business.find({"business_id": businessID}, {"name": 1, "hours": 1, "business_id": 1}):
      pprint(x)
#businessHours("CAIIPqKY0pS0yQ8fKGlDPg")

#function10 find businesses with certain number / range of reviews 
def reviewRange(low, high):
   for x in business.find({"review_count": {"$gte" : low, "$lte" : high}}, {"name": 1, "business_id": 1, "review_count": 1}).limit(10):
      pprint(x)
#reviewRange(300, 500)

#function11 find all reviews of a business
def allReviews(businessName):
   for x in business.find({"name": businessName}).limit(10):
      for y in reviews.find({"business_id": x["business_id"]}, {"text": 1}).limit(1):
         pprint(y)
#allReviews("McDonald's")

#function12 change text of review 
def changeReview(reviewID, newText):
   x = reviews.find_one({"review_id": reviewID}, {"business_id": 1})
   query = {"review_id": reviewID, "business_id": x["business_id"]}
   newValue = {"$set": {"text": newText}}
   reviews.update_one(query, newValue, upsert=True)
   getReview(reviewID)
#changeReview("TESTREVIEWID2", "changed")

#function12 find businesses with good ratings (stars greater than 3)
def goodRatings(businessID):
   for x in reviews.find({"business_id": businessID, "stars": {"$gt": 3}}, {"business_id": 1, "stars": 1, "text": 1}):
      pprint(x)
#goodRatings("k6ti2dkJD_6xGzxjQQ_FnA")

#function14 delete a review for a given business
def deleteReview(reviewID):
   x = reviews.find_one({"review_id": reviewID}, {"business_id": 1})
   query = {"review_id": reviewID, "business_id": x["business_id"]}
   reviews.delete_one(query)
   getReview(reviewID)
#deleteReview("TESTREVIEWID2")

#function15 create a review for a given business
def writeReview(reviewID, businessID, stars, text):
   review = {"review_id": reviewID, "business_id": businessID, "stars": stars, "text": text, "date": str(date.today())}
   reviews.insert_one(review)
   getReview(reviewID)
#writeReview("TESTREVIEWID2", "k6ti2dkJD_6xGzxjQQ_FnA", 5, "test review")

def getReview(reviewID):
   x = reviews.find_one({"review_id": reviewID})
   pprint(x)

#findByZip("93101")
#findByCity("Nashville")
#findByGenre("Italian")
#findByName("McDonald's")
#findByStars(5)
#findAddress("CAIIPqKY0pS0yQ8fKGlDPg")
#makeBusiness("test", "TEST BUSINESS", "1 ROAD 1", "San Jose", "CA", "11111", "1", "2", "-1", "testing, Italian", {"monday": "1:00-2:00"})
#avgReviews("McDonald's")
#businessHours("CAIIPqKY0pS0yQ8fKGlDPg")
#reviewRange(1, 500)
#allReviews("McDonald's")
#changeReview("TESTREVIEWID2", "changed")
#goodRatings("k6ti2dkJD_6xGzxjQQ_FnA")
#deleteReview("TESTREVIEWID2")
#writeReview("TESTREVIEWID2", "k6ti2dkJD_6xGzxjQQ_FnA", 5, "pls work")




#document1 = {'business_id': 'bbbbbbbbbbbbbbb', 'name': 'test'}
#business.insert_one(document1)
#print(business.find_one({"name": "test"}))


#query = {"_id": ObjectId("6376c36aac73d038b0f5691a")}
#business.delete_one(query)
#print(business.find_one({"name": "test"}))


#uQuery = {"_id": ObjectId("6376b4f5cbdd34b4b71e5fb3")}
#print(business.find_one(uQuery))
#value = {"$set": { "name": "McDonald's"} }
#business.update_one(uQuery, value)
#print(business.find_one(uQuery))