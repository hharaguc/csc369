# Holly Haraguchi
import sys

def printUsage():
   print("usage: python genDumpScript.py <DBName> <JSONFileName>")

# Prints Mongo DB connection, authentication, and insert commands to the console
def main(argv):
   if (len(argv) == 2):
      # Connect to Mongo and authenticate
      print("conn = new Mongo()")
      print("db = conn.getDB('hharaguc')")
      print("db.auth('hharaguc', 'holly96')")

      # Parse command line args
      dbNm = argv[0]
      jsonFileNm = argv[1]
      f = open(jsonFileNm, "r")
      json = f.read()
      
      print("db." + dbNm + ".insert(" + json + ")")
   else:
      printUsage() 

if __name__ == "__main__":
   main(sys.argv[1:])
