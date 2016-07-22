import json
import logging
from pprint import pprint

from bson.code import Code
from store import *

def store_to_db(filepath, store):
    """
    Reads the yelp dataset and stores the json into mongodb

    filepath: Path of the dataset
    store: Store object
    """
    objs = []       # stores all the records that have been read, storing them
                    # and then inserting in bulk would be efficient
    with open(filepath) as json_file:       # open the dataset file
        for line in json_file:              # for each line in the dataset
            data = json.loads(line)         # parse the line to get the JSON object
            objs.append(data)               # append the object to the list of objects

    print("Processed {} records from the dataset".format(len(objs)))
    store.insert_many(objs)                 # Insert all the objects into mongodb
    print("Records successfully inserted into the database")


def business_stars(store):
    """
    Counts the number of businesses with a perticular star 
    """
    mapper = Code("""
        function() {
            emit(this.stars, 1);
        }
    """)
    
    # This is the reducer Code for the map-reduce operation
    reducer = Code("""
        function(key, values) {
            var total = 0;
            for(var i=0; i<values.length; i++) {
                total += values[i];
            }
            return total;
        }
    """)
    res = store.map_reduce(mapper, reducer)
    write_output("business_stars.txt", "Count of businesses per stars", res) 
    # for row in res.find():
        # print row
    

def avg_ratings(store):
    """ 
    Calculcates and prints the average rating of the business.
    """
    mapper = Code("""
        function() {
            emit(this.business_id, parseFloat(this.stars));
        }
        """)
    
    reducer = Code("""
        function(key, values) {
            var avg = 0;
            for(var i=0; i<values.length; i++) {
                avg += values[i];
            }
            return avg/values.length;
        }
        """)
    res = store.map_reduce(mapper, reducer)
    write_output("avg_ratings.txt", "Average rating per business", res) 
    # for row in res.find():
        # print row
    
def business_categ(store):
    """ finds the number of businesses per categories """
    mapper = Code("""
        function() {
            this.categories.forEach(function(c) {
                emit(c, 1);
                });
        }
        """)
    
    reducer = Code("""
        function(key, values) {
            var total = 0;
            for(var i=0; i<values.length; i++) {
                total += values[i];
            }
            return total;
        }
        """)
    res = store.map_reduce(mapper, reducer)
    write_output("business_categ.txt", "Count of businesses per category", res) 
    # for row in res.find():
        # print row
        
        
def top_rated_business(store):
    """
    Finds the top rated business in the dataset
    """
    mapper = Code("""
        function() {
            emit(1, { maxRating: this.stars, businessId: this.business_id });
        }
        """)
    
    reducer = Code("""
        function(key, values) {
            return values.reduce(function reduce(prev, curr, i, a) {
                    if(prev.maxRating > curr.maxRating) {
                        return prev;
                    }
                    return curr;
                });
        }
        """)
    res = store.map_reduce(mapper, reducer)
    write_output("top_rated_business.txt", "Top star business", res) 
    # for row in res.find():
        # print row
        
    
def business_loc(store):
    """
    Finds the number of businesses per state(location)
    """
    mapper = Code("""
        function() {
            emit(this.state, 1);
        }
        """)
    
    reducer = Code("""
        function(key, values) {
            var total = 0;
            for(var i=0; i<values.length; i++) {
                total += values[i];
            }
            return total;
        }
        """)
    res = store.map_reduce(mapper, reducer)
    write_output("business_loc.txt", "Count of businesses per location", res) 
    # for row in res.find():
        # print row


def write_output(filename, title, res):
    with open(filename, "w") as fd:
        fd.write(title)
        fd.write("\n")
        for row in res.find():
            fd.write(str(row))
            fd.write("\n")

def main():
    store = Store()                     # The main store object
    filePath = "../dataset/yelp_academic_dataset_business.json"
    
    cmd = raw_input("Would you like to load dataset file into the database? Type 'No' if already inserted. [Yes|No] ")
    if cmd == "Yes":
        store_to_db(filePath, store)                    # Store the dataset into the mongodb
        
    print("Performing map-reduce operations on the database")
    business_stars(store)
    avg_ratings(store)
    business_categ(store)
    top_rated_business(store)
    business_loc(store)
    

if __name__ == "__main__":
    main()