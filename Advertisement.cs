using Amazon.DynamoDBv2.DataModel;
using System;
using System.Collections.Generic;

[DynamoDBTable("Advertisements")]
    public class Advertisement
    {
        public Advertisement()
        {
            CreateDate = DateTime.UtcNow;
            Id = Guid.NewGuid().ToString();
        }

        [DynamoDBHashKey]
        public string AccountId { get; set; }

        [DynamoDBRangeKey]
        public string Id { get; set; }

        [DynamoDBLocalSecondaryIndexRangeKey("Account_EndDate_Index")]
        public DateTime End { get; set; }
    }
