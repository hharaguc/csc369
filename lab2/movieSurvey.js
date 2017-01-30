// Holly Haraguchi
// Connect and authenticate
conn = new Mongo()
db = conn.getDB('hharaguc')
db.auth('hharaguc', 'holly96')

print("Query 1...\n ")
print("Query: db.survey.find( { \"respondent.gender\":\"F\" ,\"ratings.8\" : {$gte : 6} } ) ")
cursor = db.survey.find( { "respondent.gender":"F" , "ratings.8" : {$gte : 6} } )
while (cursor.hasNext()) {
	printjson(cursor.next());
}

print("\nQuery 2...\n ")
print("Query: db.survey.find( { \"respondent.state\": { $in: [\"ME\", \"NH\", \"VT\", \"MA\", \"RI\", \"NY\", \"NJ\", \"PA\", \"DE\", \"MD\", \"WV\", \"DC\"]}, \"respondent.education\" : {$gte : 4}, \"respondent.income\" : {$gte : 4} } , {\"ratings\" : 1} )")
cursor = db.survey.find( { "respondent.state": { $in: ["ME", "NH", "VT", "MA", "RI", "NY", "NJ", "PA", "DE", "MD", "WV", "DC"]}, "respondent.education" : {$gte : 4}, "respondent.income" : {$gte : 4} } , {"ratings" : 1} )
while (cursor.hasNext()) {
	printjson(cursor.next());
}

print("\nQuery 3...\n")
print("Query: db.survey.find().sort({\"ratings.1\" : -1}).limit(5)")
cursor = db.survey.find().sort({"ratings.1" : -1}).limit(5)
while (cursor.hasNext()) {
	printjson(cursor.next());
}

print("\nQuery 4...\n ")
print("Query: db.survey.find( { \"ratings.4\" : {$lt : 4}, \"ratings.3\" : {$lt : 4}, \"ratings.11\" : {$lt : 4} } ).count()")
print(db.survey.find( { "ratings.4" : {$lt : 4}, "ratings.3" : {$lt : 4}, "ratings.11" : {$lt : 4} } ).count())

print("Skipping Query 5...\n\n")

print("\nQuery 6...\n ")
print("Query: db.survey.find( { \"ratings.0\" : {$gte : 8} } ).sort({ \"respondent.age\" : -1 }).limit(6)")
cursor = db.survey.find( { "ratings.0" : {$gte : 8} } ).sort({ "respondent.age" : -1 }).limit(6)
while (cursor.hasNext()) {
	printjson(cursor.next());
}
