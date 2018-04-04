using Amazon;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Microsoft.AspNetCore.Mvc;
using Nest;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Core11Web.Controllers
{
    // this defines the schema
    public class ContentData
    {
        public ContentData()
        {
            tags = new List<string>();
        }
        public int ContentId { get; set; }  // get rid of this and query by doc id
        public string title { get; set; }
        public List<string> tags { get; set; }
        public string content { get; set; }
        public string media { get; set; }
    }
    // for the SNS publish
    public class SnsContentData
    {
        public SnsContentData()
        {
            tags = new List<string>();
        }
        public int id { get; set; }
        public int ContentId { get; set; }  // get rid of this and query by doc id
        public string title { get; set; }
        public List<string> tags { get; set; }
        public string content { get; set; }
        public string media { get; set; }
    }

    public class HomeController : Controller
    {
        public static Uri node;
        public static ConnectionSettings settings;
        public static ElasticClient client;
       

        // check out this handy site
        // https://hassantariqblog.wordpress.com/category/back-end-stuff/elastic-search/

        // and the .net NEST lib is in.  https://github.com/elastic/elasticsearch-net
        private void TestElasticSearch()
        {
            node = new Uri("https://search-testtbk-vibxsyhb7jpdbcth5337jovgl4.us-east-1.es.amazonaws.com");

            settings = new ConnectionSettings(node);
            settings.DefaultIndex("contentdb");
            client = new ElasticClient(settings);

            // Mapping for indexes
            var indexSettings = new IndexSettings();
            indexSettings.NumberOfReplicas = 1;
            indexSettings.NumberOfShards = 1;
            // Create properties from the Post class
            // https://www.elastic.co/guide/en/elasticsearch/client/net-api/current/fluent-mapping.html

            CreateAndLoad();            
            TestUpsert();
            //TestPushToESViaSNS();
        }

        private void TestUpsert()
        {
            var contentDoc = new ContentData
            {
                title = "C# title updating - 2",
                content = "This is from C# upsert - 21",
                media = "this is media"
            };
            contentDoc.tags.Add("Tag1");
            contentDoc.tags.Add("Tag2");
            contentDoc.tags.Add("Happiness");
            contentDoc.tags.Add("Tag3");

            var response = client.Update(DocumentPath<ContentData>
                .Id(101),
                u => u
                    .Index("contentdb")
                    .Type("2")  
                    .DocAsUpsert(true)
                    .Doc(contentDoc));
            Console.WriteLine(response);


            // Do some queries

            string[] matchTerms =
            {
                "The quick",  // will find two entries.  Two with "the" and one with "quick"(but that has "the" as well with a score of 2)
                "Football",
                "Hockey",
                "Chicago Bears",
                "St. Louis"
            };

            // Match terms would come from what the user typed in
            foreach (var term in matchTerms)
            {
                var result = client.Search<ContentData>(s =>
                   s
                   .From(0)
                   .Size(10000)
                   .Index("contentdb")
                   .Type("2")   // Client Id = 2
                   .Query(q => q.Match(mq => mq.Field(f => f.content).Query(term))));
                // print out the result.
            }
        }

        public void TestPushToESViaSNS()
        {
            // https://docs.aws.amazon.com/sdk-for-net/v2/developer-guide/SendMessage.html

             AmazonSimpleNotificationServiceClient snsClient = new AmazonSimpleNotificationServiceClient(
                AccessKeyAtBottom,
                SecretKeyAtBottom,
                RegionEndpoint.USEast1);

            SnsContentData data = new SnsContentData()
            {
                id = 100,
                ContentId = 7777,
                content = "This is SNS content from what Umbraco published. ",
                media = "Media from what Umbraco published",
                tags = new List<string>()
                {
                    "umbraco1",
                    "umbraco2",
                    "Umbraco3"
                },
                title = "this is an umbraco title to change from SNS"
            };
            string umbracoContent = JsonConvert.SerializeObject(data);
            /*
             * https://aws.amazon.com/sns/faqs/
                Lambda: If Lambda is not available, SNS will retry 2 times at 1 seconds apart, then 10 times exponentially backing off from 1 seconds to 20 minutes and finally 38 times every 20 minutes for a total 50 attempts over more than 13 hours before the message is discarded from SNS.              
             */
            var response = snsClient.PublishAsync(
                new PublishRequest()
                {
                    TopicArn = "arn:aws:sns:us-east-1:109883045809:PubToElasticSearch",
                    Subject = "Umbraco Push Notification",
                    Message = umbracoContent
                }).Result;

            // https://aws.amazon.com/sns/faqs/

            if (response != null) // 200 is success
            {
                Console.WriteLine("Please hit return...");
                Console.ReadLine();
            }
        }

        private void CreateAndLoad()
        {
            // after creating this, you can issue
            // GET /myindex    in the command area.
            // https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-index.html

            if (client.IndexExists("contentdb").Exists)
            {
                TestDeleteIndex();
            }
            //var createIndexResponse = client.CreateIndex("contentdb", c => c
            //        .Mappings(ms => ms
            //            .Map<ContentData>(m => m
            //                .AutoMap(typeof(ContentData))
            //            )
            //        )
            //);
            // todo, check createIndexResponse

            TestInsert();

            TestTermQuery();

            TestMatchPhrase();

            TestFilter();
        }

        private void TestTermQuery()
        {
            // Uncomment this out
            var result = client.Search<ContentData>(s =>
                s.From(0).Size(10000).Type("2").Query(q => q.Term(t => t.ContentId, 1)));
            /*
            GET contentidx/content/_search
            {
              "query": {
                "match":{
                  "contentText":"Louis"
                }
              }
            }
             */

            string[] matchTerms =
            {
                "The quick",  // will find two entries.  Two with "the" and one with "quick"(but that has "the" as well with a score of 2)
                "Football",
                "Hockey",
                "Chicago Bears",
                "St. Louis"
            };

            // Match terms would come from what the user typed in
            foreach (var term in matchTerms)
            {
                result = client.Search<ContentData>(s =>
                   s
                   .From(0)
                   .Size(10000)
                   .Index("contentdb")
                   .Type("2")   // Client Id = 2
                   .Query(q => q.Match(mq => mq.Field(f => f.content).Query(term))));
                // print out the result.
            }
        }

        private void TestMatchPhrase()
        {
            // Exact phrase matching
            string[] matchPhrases =
            {
                "The quick",
                "Louis Blues",
                "Chicago Bears"
            };

            // Match terms would come from what the user typed in
            foreach (var phrase in matchPhrases)
            {
                var result = client.Search<ContentData>(s =>
                   s
                   .From(0)
                   .Size(10000)
                   .Index("contentdb")
                   .Type("2")   // client id = 2
                   .Query(q => q.MatchPhrase(mq => mq.Field(f => f.content).Query(phrase))));
                // print out the result.
            }
        }

        private void TestFilter()
        {
            var result = client.Search<ContentData>(s =>
                s
                .From(0)
                .Size(10000)
                .Index("contentdb")
                .Type("2")  // client id = 2
                .Query(q => q
                    .Bool(b => b
                        // todo: get rid of this ContentId and search on the incoming id with the document id
                        .Filter(filter => filter.Range(m => m.Field(fld => fld.ContentId).GreaterThanOrEquals(4)))
                        )
                    ));
            // print out the result.            
        }

        private void TestInsert()
        {
            // Insert data

            string[] contentText =
            {
                "<p>Chicago Cubs Baseball</p>",
                "<html><body><p>St. Louis Cardinals Baseball</p></body></html>",
                "St. Louis Blues Hockey",
                "The Chicago Bears Football",
                "The quick fox jumped over the lazy dog"
            };

            int idx = 1;
            foreach (var text in contentText)
            {

                var contentDoc = new ContentData
                {
                    ContentId = idx,    // todo, is it ok to have this ContentId and the Id be the same?
                    title = "test title " + idx,
                    content = text,
                    media = "this is media"
                };
                contentDoc.tags.Add("Tag1");
                contentDoc.tags.Add("Tag2");
                contentDoc.tags.Add("Tag3");
                contentDoc.tags.Add("Tag4");


                var response = client.Update(DocumentPath<ContentData>
                    .Id(idx++),
                    u => u
                        .Index("contentdb")
                        .Type("2")  // Simulate client id  = 2 as the type
                        .DocAsUpsert(true)
                        .Doc(contentDoc));

                // this will insert
                // See https://hassantariqblog.wordpress.com/2016/09/21/elastic-search-insert-documents-in-index-using-nest-in-net/
                // Not calling this
                //client.Index(simulatedContentFromDB, i => i.Index("contentidx"));

            }

            // To confirm you added data from "Content", you can type this in
            // GET contentindex/_search
        }

        public object TestDeleteIndex()
        {
            var response = client.DeleteIndex("contentdb");
            return response;
        }

        public IActionResult Index()
        {

            TestElasticSearch();
            return View();
        }

        public IActionResult About()
        {
            ViewData["Message"] = "Your application description page.";

            return View();
        }

        public IActionResult Contact()
        {
            ViewData["Message"] = "Your contact page.";

            return View();
        }

        public IActionResult Error()
        {
            return View();
        }
        public static string AccessKeyAtBottom = Your AWS API KEY here
        public static string SecretKeyAtBottom = Your AWS client secret here;
    }        
}


/*************  

GET _search
{
  "query": {
    "match_all": {}
  }
}

DELETE /contentdb

*/
