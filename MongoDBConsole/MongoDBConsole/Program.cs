using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoDBConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync().Wait();

            Console.WriteLine("test");

            Console.ReadLine();
        }

        static BsonDocument[] GetValues()
        {
            string[] lines = System.IO.File.ReadAllLines(@"permits_ottawa.json");
            BsonDocument[] Bsonlines = new BsonDocument[lines.Length];

            int compt = 0;
            foreach(string line in lines)
            {
                Bsonlines[compt] = BsonDocument.Parse(line);
            }

            return Bsonlines;
        }

        static async Task MainAsync()
        {
            //BsonDocument.Parse(json)

            var connectionString = "mongodb://localhost:27017";

            var client = new MongoClient(connectionString);

            IMongoDatabase db = client.GetDatabase("school");


            IMongoCollection<BsonDocument> collection = db.GetCollection<BsonDocument>("students");


            /////////////////////////////////////////////// Insert Databases Values /////////////////////////////////////////////////////////////////////////

            BsonDocument[] Bsonlines = GetValues();

            foreach(BsonDocument data in Bsonlines)
            {
                Console.WriteLine(data.ToString());
                await collection.InsertOneAsync(data);
            }

        }
    
    }
}
