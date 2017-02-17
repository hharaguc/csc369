// Holly Haraguchi
// Lab 4
// CSC 369, Winter 2016

// Connect and authenticate
conn = new Mongo()
db = conn.getDB('hharaguc')
db.auth('hharaguc', 'holly96')

// ThghtShre Queries
print("Query 1...\n")
print("db.thght.aggregate([{ \"$group\" : { \"_id\" : \"$status\" , \"messages\" : { \"$sum\" : { \"$literal\" : 1 } } } } , { \"$sort\" : { messages : -1 } } , { \"$limit\" : 1 } , { \"$project\" : { \"status\" : \"$_id\", \"messages\" : \"$messages\", \"_id\" : 0 } } ])")

cursor = db.thght.aggregate([
                     { "$group" : { "_id" : "$status" , "messages" : { "$sum" : { "$literal" : 1 } } } } ,
                     { "$sort" : { messages : -1 } } ,
                     { "$limit" : 1 } , 
                     { "$project" : { "status" : "$_id", "messages" : "$messages", "_id" : 0 } }
                  ])
while (cursor.hasNext()) {
	printjson(cursor.next());
}

print("\nQuery 2...\n")
print("db.thght.aggregate([{ \"$group\" : { \"_id\" : \"$user\" , \"recipients\" : { \"$addToSet\" : \"$recipient\" } } } , { \"$project\" : { \"user\" : \"$_id\", \"recipients\" : \"$recipients\", \"_id\" : 0 } } ])")
cursor = db.thght.aggregate([
                     { "$group" : { "_id" : "$user" , "recipients" : { "$addToSet" : "$recipient" } } } ,
                     { "$project" : { "user" : "$_id", "recipients" : "$recipients", "_id" : 0 } }
                  ])
while (cursor.hasNext()) {
	printjson(cursor.next());
}

print("\nQuery 3...\n")
print("db.thght.aggregate([{ \"$group\" : { \"_id\" : \"$user\" , \"recipients\" : { \"$addToSet\" : \"$recipient\" } } } , { \"$project\" : { \"user\" : \"$_id\", \"recipients\" : { \"$size\" : \"$recipients\" }, \"_id\" : 0 } } ])") 
cursor = db.thght.aggregate([
                     { "$group" : { "_id" : "$user" , "recipients" : { "$addToSet" : "$recipient" } } } ,
                     { "$project" : { "user" : "$_id", "recipients" : { "$size" : "$recipients" }, "_id" : 0 } }
                  ])
while (cursor.hasNext()) {
	printjson(cursor.next());
}

print("\nQuery 4...\n")
print("db.thght.aggregate([{ \"$match\" : { recipient : \"self\" } } , { \"$group\" : { \"_id\" : \"$status\" , \"count\" : { \"$sum\": { \"$literal\" : 1 } } } } , { \"$sort\" : { count : -1 } } , { \"$limit\" : 1 } , { \"$project\" : { \"status\" : \"$_id\" , \"selfAddressed\" : \"$count\", \"_id\" : 0 } } ])")
cursor = db.thght.aggregate([
                     { "$match" : { recipient : "self" } } ,
                     { "$group" : { "_id" : "$status" , "count" : { "$sum": { "$literal" : 1 } } } } ,
                     { "$sort" : { count : -1 } } , 
                     { "$limit" : 1 } ,
                     { "$project" : { "status" : "$_id" , "selfAddressed" : "$count", "_id" : 0 } }
                  ])
while (cursor.hasNext()) {
	printjson(cursor.next());
}

print("\nQuery 5...\n")
print("db.thght.aggregate([{ \"$group\" : { \"_id\" : \"$user\" , \"msgCnt\" : { \"$sum\" : { \"$literal\" : 1 } } , \"texts\" : { \"$push\" : \"$text\" } } } , { \"$match\" : { \"msgCnt\" : { \"$gt\" : 2 } } } , { \"$project\" : { \"user\" : \"$_id\", \"text\" : { \"$arrayElemAt\" : [\"$texts\", 1] }, \"_id\" : 0 } } , { \"$sort\" : { user : 1 } } ])")

cursor = db.thght.aggregate([
                     { "$group" : { "_id" : "$user" , "msgCnt" : { "$sum" : { "$literal" : 1 } } , "texts" : { "$push" : "$text" } } } ,
                     { "$match" : { "msgCnt" : { "$gt" : 2 } } } ,
                     { "$project" : { "user" : "$_id", "text" : { "$arrayElemAt" : ["$texts", 1] }, "_id" : 0 } } , 
                     { "$sort" : { user : 1 } }
                  ])
while (cursor.hasNext()) {
	printjson(cursor.next());
}
