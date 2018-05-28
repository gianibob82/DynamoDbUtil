# DynamoDbUtil

Aspnet Core c# utility class that uses reflection to create DynamoDb tables with fields and indexes. It could work in asp.net too but you just need to change referenced libraries.

Instantiate it and call it referencing the assembly containing the table models, like 
`await _dynamoDbService.CreateTablesAsync(typeof(Advertisement).Assembly);`

I've provided the table in the example.


