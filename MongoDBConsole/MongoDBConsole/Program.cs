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

            var connectionString = "mongodb://localhost:27017";

            var client = new MongoClient(connectionString);

            IMongoDatabase db = client.GetDatabase("OTTAWA");


            IMongoCollection<BsonDocument> collection = db.GetCollection<BsonDocument>("permits");


           


            Console.WriteLine("test");

            var caseSwitch = Console.ReadKey();

            Console.WriteLine("Press 0 for reformating the JSON file to fit in the cassandra database, 1 for Simple Queries, 2 for Complex Queries, 3 for Hard Query");

            switch (caseSwitch.Key.ToString())
            {
                case "1":
                    Console.WriteLine("Simple Queries");
                    SimpleQueriesAsync(collection).Wait();
                    break;
                case "2":
                    Console.WriteLine("Complex Queries");
                    ComplexQueriesAsync(collection).Wait();
                    break;
                case "3":
                    Console.WriteLine("Hard Query");
                    HardQueryAsync(collection).Wait();
                    break;
                case "0":
                    Console.WriteLine("Formatting JSON file to insert in cassandra");
                    Insert_Values(collection).Wait();
                    break;
            }
            //SimpleQueries(session);
            //ComplexQueries(session);
            //HardQuery(session);

            Console.ReadLine();
        }
        static async Task SimpleQueriesAsync(IMongoCollection<BsonDocument> collection)
        {
            Console.WriteLine("First Easy Query : select housenumber, road from ottawa_permits limit 10\n");

            var filter = "{{}, {housenumber : 1, road : 1, _id : 0}}";

            await collection.Find(filter)
                .ForEachAsync(document => Console.WriteLine(document));

            Console.WriteLine("\nSecond Easy Query : select municipality from ottawa_permits where year = 2011 LIMIT 10 ALLOW FILTERING\n");

            var filter2 = "{{year : 2011}, {municipality : 1, _id : 0}}";

            await collection.Find(filter2)
                .ForEachAsync(document => Console.WriteLine(document));

            //Main(null);
        }

        static async Task ComplexQueriesAsync(IMongoCollection<BsonDocument> collection)
        {
            Console.WriteLine("First Complex Query : SELECT permits.filename from ottawa_permits where month='July' limit 10 ALLOW FILTERING\n");
            var filter = "{{month : 'July'}, {'permits.filename' : 1, _id : 0}}";

            await collection.Find(filter)
                .ForEachAsync(document => Console.WriteLine(document));


            Console.WriteLine("\nSecond Complex Query : select id.oid, municipality, permits.contractor from ottawa_permits where housenumber > 500 LIMIT 10 ALLOW FILTERING\n");

            var filter2 = "{{housenumber : {$gt : 500}}, {municipality : 1, 'permits.CONTRACTOR' : 1, _id : 1}}";

            await collection.Find(filter2)
                .ForEachAsync(document => Console.WriteLine(document));

            //Main(null);
        }

        static async Task HardQueryAsync(IMongoCollection<BsonDocument> collection)
        {
            Console.WriteLine("Hard Query : select permits.contractor, municipality from ottawa_permits where year = 2011 limit 20 ALLOW FILTERING\n");

            var filter = "{{year : 2011}, {municipality : 1, 'permits.CONTRACTOR' : 1, _id : 1}}";

            await collection.Find(filter)
                .ForEachAsync(document => Console.WriteLine(document));

            //Main(null);
        }


        static BsonDocument[] GetValues()
        {
            string[] lines = System.IO.File.ReadAllLines(@"permits_ottawa.json");
            BsonDocument[] Bsonlines = new BsonDocument[lines.Length];

            int compt = 0;
            foreach(string line in lines)
            {
                Bsonlines[compt] = BsonDocument.Parse(line);
                compt++;
            }

            return Bsonlines;
        }

        static async Task Insert_Values(IMongoCollection<BsonDocument> collection)
        {
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
