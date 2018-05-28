using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

public class DynamoDbUtils
    {
        readonly IAmazonDynamoDB _oDynamoDBClient;
        readonly IDynamoConfig _dynamoConfig;

        public DynamoDbUtils(IAmazonDynamoDB oDynamoDb, IDynamoConfig dynamoConfig )
        {
            this._oDynamoDBClient = oDynamoDb;
            this._dynamoConfig = dynamoConfig;
        }
        
        /// <summary>
        /// Creates tables that don't exist, with LocalIndexes and GlobalIndexes when found
        /// </summary>
        /// <param name="assembly">The assembly that contains DynamoDb entities</param>
        /// <returns></returns>
        public async Task<StringBuilder> CreateTablesAsync(Assembly assembly)
        {
            StringBuilder oSb = new StringBuilder();

            var currentTables = await _oDynamoDBClient.ListTablesAsync();

            var tableNames = currentTables.TableNames;

            var types = GetTypesWithDynamoTableAttribute(assembly);

            var tablePrefix = _dynamoConfig.TablePrefix;

            foreach (var tbl in types)
            {
                var tblName = tablePrefix + tbl.GetCustomAttribute<DynamoDBTableAttribute>().TableName;
                if (!tableNames.Contains(tblName))
                {
                    Projection projection = new Projection() { ProjectionType = "INCLUDE" };
                    List<string> nonKeyAttributes = tbl.GetProperties().Where(p => p.GetCustomAttribute<DynamoDBPropertyAttribute>() != null).Select(s => s.Name).ToList();
                    projection.NonKeyAttributes = nonKeyAttributes;

                    CreateTableRequest reqCreateTable = new CreateTableRequest();
                    reqCreateTable.TableName = tblName;
                    reqCreateTable.ProvisionedThroughput = new ProvisionedThroughput();
                    reqCreateTable.ProvisionedThroughput.ReadCapacityUnits = 1;
                    reqCreateTable.ProvisionedThroughput.WriteCapacityUnits = 1;

                    var hashKeyProperty = tbl.GetProperties().FirstOrDefault(p => p.GetCustomAttribute<DynamoDBHashKeyAttribute>() != null);

                    if (hashKeyProperty != null)
                    {
                        string hashKeyName = hashKeyProperty.Name;
                        if (hashKeyProperty.GetCustomAttribute<DynamoDBHashKeyAttribute>().AttributeName != null)
                            hashKeyName = hashKeyProperty.GetCustomAttribute<DynamoDBHashKeyAttribute>().AttributeName;

                        var hashKeySchema = new KeySchemaElement() { AttributeName = hashKeyName, KeyType = KeyType.HASH };
                        reqCreateTable.KeySchema.Add(hashKeySchema);

                        reqCreateTable.AttributeDefinitions.Add(ReturnAttributeDefinition(hashKeyProperty, hashKeyName));

                        // adding range property if exists
                        var rangeyProperty = tbl.GetProperties().Where(p => p.GetCustomAttribute<DynamoDBRangeKeyAttribute>() != null).FirstOrDefault();

                        if (rangeyProperty != null)
                        {
                            string rangeKeyName = rangeyProperty.Name;
                            if (rangeyProperty.GetCustomAttribute<DynamoDBRangeKeyAttribute>().AttributeName != null)
                                rangeKeyName = rangeyProperty.GetCustomAttribute<DynamoDBRangeKeyAttribute>().AttributeName;

                            reqCreateTable.KeySchema.Add(new KeySchemaElement() { AttributeName = rangeKeyName, KeyType = KeyType.RANGE });
                            reqCreateTable.AttributeDefinitions.Add(ReturnAttributeDefinition(rangeyProperty, rangeKeyName));

                            // adding local secondary indexes
                            var localIndexes = tbl.GetProperties().Where(p => p.GetCustomAttribute<DynamoDBLocalSecondaryIndexRangeKeyAttribute>() != null);
                            foreach (var indexProperty in localIndexes)
                            {
                                // usually only one index is assigned
                                string indexName = indexProperty.GetCustomAttribute<DynamoDBLocalSecondaryIndexRangeKeyAttribute>().IndexNames.First();
                                List<KeySchemaElement> indexKeySchema = new List<KeySchemaElement>();
                                indexKeySchema.Add(hashKeySchema);
                                indexKeySchema.Add(new KeySchemaElement() { AttributeName = indexProperty.Name, KeyType = KeyType.RANGE });

                                reqCreateTable.AttributeDefinitions.Add(ReturnAttributeDefinition(indexProperty, indexProperty.Name));

                                LocalSecondaryIndex localSecondaryIndex = new LocalSecondaryIndex()
                                {
                                    IndexName = indexName,
                                    KeySchema = indexKeySchema,
                                    Projection = projection
                                };

                                reqCreateTable.LocalSecondaryIndexes.Add(localSecondaryIndex);
                            }
                        }

                        // adding local secondary indexes
                        var globalIndexes = tbl.GetProperties().Where(p => p.GetCustomAttribute<DynamoDBGlobalSecondaryIndexHashKeyAttribute>() != null);
                        if (globalIndexes.Count() > 0)
                        {
                            foreach (var globalIndexProperty in globalIndexes)
                            {
                                var globalKeySchema = new List<KeySchemaElement>();

                                string globalHashKeyName = globalIndexProperty.Name;
                                if (globalIndexProperty.GetCustomAttribute<DynamoDBGlobalSecondaryIndexHashKeyAttribute>().AttributeName != null)
                                    globalHashKeyName = globalIndexProperty.GetCustomAttribute<DynamoDBGlobalSecondaryIndexHashKeyAttribute>().AttributeName;

                                var globalHashKeySchema = new KeySchemaElement() { AttributeName = globalHashKeyName, KeyType = KeyType.HASH };
                                globalKeySchema.Add(globalHashKeySchema);
                                reqCreateTable.AttributeDefinitions.Add(ReturnAttributeDefinition(globalIndexProperty, globalHashKeyName));

                                string thisGlobalIndexName = globalIndexProperty.GetCustomAttribute<DynamoDBGlobalSecondaryIndexHashKeyAttribute>().IndexNames.First();

                                // find the range keys related to this index
                                var globalRangeProperty = tbl.GetProperties().SingleOrDefault(p => p.GetCustomAttribute<DynamoDBGlobalSecondaryIndexRangeKeyAttribute>() != null && p.GetCustomAttribute<DynamoDBGlobalSecondaryIndexRangeKeyAttribute>().IndexNames.Contains(thisGlobalIndexName));
                                if (globalRangeProperty != null)
                                {
                                    string globalRangeKeyName = globalRangeProperty.Name;
                                    if (globalRangeProperty.GetCustomAttribute<DynamoDBGlobalSecondaryIndexRangeKeyAttribute>().AttributeName != null)
                                        globalRangeKeyName = globalRangeProperty.GetCustomAttribute<DynamoDBGlobalSecondaryIndexRangeKeyAttribute>().AttributeName;

                                    var globalRangeKeySchema = new KeySchemaElement() { AttributeName = globalRangeKeyName, KeyType = KeyType.RANGE };
                                    globalKeySchema.Add(globalRangeKeySchema);
                                    reqCreateTable.AttributeDefinitions.Add(ReturnAttributeDefinition(globalRangeProperty, globalRangeKeyName));
                                }

                                GlobalSecondaryIndex globalSecondaryIndex = new GlobalSecondaryIndex()
                                {
                                    IndexName = thisGlobalIndexName,
                                    KeySchema = globalKeySchema,
                                    Projection = projection,
                                     ProvisionedThroughput = new ProvisionedThroughput(1,1)
                                };

                                reqCreateTable.GlobalSecondaryIndexes.Add(globalSecondaryIndex);
                            }
                        }

                        var response = await _oDynamoDBClient.CreateTableAsync(reqCreateTable);

                        // tables aren't created instantly
                        await WaitUntilTableReadyAsync(reqCreateTable.TableName);

                        foreach (var kv in response.ResponseMetadata.Metadata)
                            oSb.AppendLine($"{kv.Key} {kv.Value}");
                    }
                    else
                    {
                        oSb.AppendLine($"{tblName} table has no hash key property");
                    }
                }
            }
            return oSb;
        }

        public async Task DropAllTables(string tablePrefix)
        {
            var currentTables = await _oDynamoDBClient.ListTablesAsync();
              var tableNames = currentTables.TableNames.Where(t => t.StartsWith(tablePrefix));

            foreach (var table in tableNames)
            {
                DeleteTableRequest deleteTableRequest = new DeleteTableRequest(table);
                var result = _oDynamoDBClient.DeleteTableAsync(deleteTableRequest);
                Thread.Sleep(10000);
            }
        }

        private List<Type> GetTypesWithDynamoTableAttribute(Assembly assembly)
        {
            List<Type> types = new List<Type>();

            foreach (Type type in assembly.GetTypes())
            {
                if (type.GetCustomAttributes(typeof(DynamoDBTableAttribute), true).Length > 0)
                {
                    types.Add(type);
                }
            }

            return types;
        }

        private AttributeDefinition ReturnAttributeDefinition(PropertyInfo property, string propertyName)
        {
            if (property.GetMethod.ReturnType == typeof(string) || property.GetMethod.ReturnType == typeof(DateTime))
                return new AttributeDefinition(propertyName, ScalarAttributeType.S);
            else
                return new AttributeDefinition(propertyName, ScalarAttributeType.N);
        }

        private async Task WaitUntilTableReadyAsync(string tableName)
        {
            string status = null;
            // Let us wait until table is created. Call DescribeTable.
            do
            {
                System.Threading.Thread.Sleep(5000); // Wait 5 seconds.
                try
                {
                    var res = await _oDynamoDBClient.DescribeTableAsync(new DescribeTableRequest
                    {
                        TableName = tableName
                    });

                    Console.WriteLine("Table name: {0}, status: {1}",
                              res.Table.TableName,
                              res.Table.TableStatus);
                    status = res.Table.TableStatus;
                }
                catch (ResourceNotFoundException)
                {
                    // DescribeTable is eventually consistent. So you might
                    // get resource not found. So we handle the potential exception.
                }
            } while (status != "ACTIVE");
        }
    }
