// Holly Haraguchi
// Lab 4
// CSC 369, Winter 2016

// Connect and authenticate
conn = new Mongo()
db = conn.getDB('hharaguc')
db.auth('hharaguc', 'holly96')

// MovieSurvey Queries
print("Query 1...\n")
print("db.survey.aggregate ([{ \"$unwind\": \"$ratings\"} , { \"$group\": { _id: \"$_id\", name: { $first: \"$respondent.name\" }, avgscore: { $avg: \"$ratings\" } } } , { \"$project\": {name: { first: \"$name.first\" , last: \"$name.last\" } , avgscore: \"$avgscore\"} } ])")
cursor = db.survey.aggregate ([   
                        { "$unwind": "$ratings"} , 
                        { "$group": { _id: "$_id", name: { $first: "$respondent.name" }, avgscore: { $avg: "$ratings" } } } ,
                        { "$project": 
                            { 
                                name: { first: "$name.first" , last: "$name.last" } ,
                                avgscore: "$avgscore" 
                            }
                        } 
                    ])
while (cursor.hasNext()) {
	printjson(cursor.next());
}

print("\nQuery 2...\n")
print("db.survey.aggregate([{ \"$redact\": {\"$cond\": {\"if\": {\"$gt\": [{ \"$arrayElemAt\": [ \"$ratings\", 3 ] } , { \"$arrayElemAt\": [ \"$ratings\", 9 ] } ] } , \"then\": \"$$KEEP\" , \"else\": \"$$PRUNE\"} } } , { \"$project\": {name: { first: \"$respondent.name.first\" , last : \"$respondent.name.last\" } , memento: { $arrayElemAt: [\"$ratings\", 3] } , dogma: { $arrayElemAt: [\"$ratings\", 9] } } } , { \"$sort\": { memento: -1 } } ])")
cursor = db.survey.aggregate([
                     { "$redact": 
                        {
                           "$cond": 
                           {
                              "if": 
                              { 
                                 "$gt": 
                                    [
                                      { "$arrayElemAt": [ "$ratings", 3 ] } ,
                                      { "$arrayElemAt": [ "$ratings", 9 ] }
                                    ]
                              } ,
                              "then": "$$KEEP" ,
                              "else": "$$PRUNE"
                           }
                        }
                     } ,
                     { "$project":
                        {
                           name: { first: "$respondent.name.first" , last : "$respondent.name.last" } ,
                           memento: { $arrayElemAt: ["$ratings", 3] } ,
                           dogma: { $arrayElemAt: ["$ratings", 9] }
                        }
                     } ,
                     { "$sort": { memento: -1 } }
                  ])
while (cursor.hasNext()) {
	printjson(cursor.next());
}

print("\nQuery 3...\n")
print("db.survey.aggregate([{ \"$group\": {\"_id\" : \"Princess Bride\", \"ratings\" : { \"$push\": { $arrayElemAt: [\"$ratings\", 5] } } } } ])")
cursor = db.survey.aggregate([
                     { "$group": 
                        { 
                           "_id" : "Princess Bride", "ratings" : { "$push": { $arrayElemAt: ["$ratings", 5] } }
                        }
                     }
                  ])
while (cursor.hasNext()) {
	printjson(cursor.next());
}

print("\nQuery 4...\n")
print("db.survey.aggregate([{ \"$unwind\" : \"$ratings\" } , { \"$match\" :  { \"ratings\": { \"$gt\" : 8} } } , { \"$project\": {count: { \"$literal\": 1 } , respondent: 1 , ratings: 1 } } , { \"$group\": {\"_id\": \"$_id\" , \"respondent\": { \"$first\" : \"$respondent\" } , \"count\": { \"$sum\" : \"$count\" } } } , { \"$group\": {\"_id\" : \"$count\", \"users\" : { \"$push\": { \"id\" : \"$_id\", \"name\" : { \"first\": \"$respondent.name.first\", \"last\" : \"$respondent.name.last\" }  } } } } , { \"$sort\" : { _id : -1} } , { \"$limit\" : 1 } , { \"$unwind\" : \"$users\"} , { \"$project\" : { \"_id\" : \"$users.id\", \"name\" : \"$users.name\" } } ])")
cursor = db.survey.aggregate([
                     { "$unwind" : "$ratings" } ,
                     { "$match" :  { "ratings": { "$gt" : 8} } } ,
                     { "$project": 
                        { 
                           count: { "$literal": 1 } ,
                           respondent: 1 ,
                           ratings: 1 
                        }
                     } ,
                     { "$group": 
                        { 
                           "_id": "$_id" , 
                           "respondent": { "$first" : "$respondent" } ,
                           "count": { "$sum" : "$count" }
                        } 
                     } , 
                     { "$group":
                        {
                           "_id" : "$count",
                           "users" : { "$push": { "id" : "$_id", "name" : { "first": "$respondent.name.first", "last" : "$respondent.name.last" }  } }
                        }
                     } ,
                     { "$sort" : { _id : -1} } ,
                     { "$limit" : 1 } ,
                     { "$unwind" : "$users"} ,
                     { "$project" : { "_id" : "$users.id", "name" : "$users.name" } }
                  ])
while (cursor.hasNext()) {
	printjson(cursor.next());
}

print("\nQuery 5...\n")
print("db.survey.aggregate([{ \"$unwind\" : { path: \"$ratings\", includeArrayIndex: \"movieIdx\" } } ,{ \"$group\" : { \"_id\" : { gender: \"$respondent.gender\", movieIdx: \"$movieIdx\" } , \"avgRating\" : { \"$avg\" : \"$ratings\" } } } ,{ \"$group\" : { \"_id\" : \"$_id.gender\" , \"maxAvgRating\" : { \"$max\" : \"$avgRating\" } , \"movies\" : { \"$push\" : { \"movieIdx\": \"$_id.movieIdx\", \"avgRating\": \"$avgRating\" } } } } ,{ \"$project\" : { movies : {\"$filter\" : { input: \"$movies\", as: \"movie\", cond: { $eq: [ \"$$movie.avgRating\", \"$maxAvgRating\" ] } } } } } ,{ \"$unwind\" : \"$movies\"} ,{ \"$project\" : { \"gender\" : \"$_id\", \"favorite\" : \"$movies.movieIdx\", \"score\" : \"$movies.avgRating\" } } ])")
cursor = db.survey.aggregate([
                     { "$unwind" : { path: "$ratings", includeArrayIndex: "movieIdx" } } ,
                     { "$group" : { "_id" : { gender: "$respondent.gender", movieIdx: "$movieIdx" } , "avgRating" : { "$avg" : "$ratings" } } } ,
                     { "$group" : { "_id" : "$_id.gender" , "maxAvgRating" : { "$max" : "$avgRating" } , 
                           "movies" : { "$push" : { "movieIdx": "$_id.movieIdx", "avgRating": "$avgRating" } } } } ,
                     { "$project" : { movies : {"$filter" : { input: "$movies", as: "movie", cond: { $eq: [ "$$movie.avgRating", "$maxAvgRating" ] } } } } } ,
                     { "$unwind" : "$movies"} ,
                     { "$project" : { "gender" : "$_id", "favorite" : "$movies.movieIdx", "score" : "$movies.avgRating" } }                
                  ])
while (cursor.hasNext()) {
	printjson(cursor.next());
}

