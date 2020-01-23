package com.example.accessingdatacosmosmongodb;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MappedDocument;
import org.springframework.data.mongodb.core.MongoAction;
import org.springframework.data.mongodb.core.MongoActionOperation;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.*;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CustomMongoTemplate extends MongoTemplate {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomMongoTemplate.class);
    private MongoClient customMongoClient;
    private String databaseName;
    public CustomMongoTemplate(MongoClient mongoClient, String databaseName) {

        super(mongoClient, databaseName);
        this.customMongoClient=mongoClient;
        this.databaseName=databaseName;

    }

    private static MongoConverter getDefaultMongoConverter(MongoDbFactory factory) {
        DbRefResolver dbRefResolver = new DefaultDbRefResolver(factory);
        MongoCustomConversions conversions = new MongoCustomConversions(Collections.emptyList());
        MongoMappingContext mappingContext = new MongoMappingContext();
        mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
        mappingContext.afterPropertiesSet();
        MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, mappingContext);
        converter.setCustomConversions(conversions);
        converter.setCodecRegistryProvider(factory);
        converter.afterPropertiesSet();
        return converter;
    }


    @Override
    protected Object insertDocument(String collectionName, Document document, Class<?> entityClass) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Inserting Document containing fields: {} in collection: {}", document.keySet(), collectionName);
        }

        return this.execute(collectionName, (collection) -> {
            MongoAction mongoAction = new MongoAction(null, MongoActionOperation.INSERT, collectionName, entityClass, document, (Document)null);
            WriteConcern writeConcernToUse = this.prepareWriteConcern(mongoAction);
            if (writeConcernToUse == null) {
                collection.insertOne(document);
            } else {
                collection.withWriteConcern(writeConcernToUse).insertOne(document);
            }

            return document.get("_id");
        });
    }

    @Override
    protected List<Object> insertDocumentList(String collectionName, List<Document> documents) {
        if (documents.isEmpty()) {
            return Collections.emptyList();
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Inserting list of Documents containing {} items", documents.size());
            }

            this.execute(collectionName, (collection) -> {
                MongoAction mongoAction = new MongoAction(null, MongoActionOperation.INSERT_LIST, collectionName, (Class)null, (Document)null, (Document)null);
                WriteConcern writeConcernToUse = this.prepareWriteConcern(mongoAction);
                if (writeConcernToUse == null) {
                    try {
                        InsertDocumentsInParallel(this.customMongoClient,this.databaseName,collectionName,null,documents);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    //collection.insertMany(documents);
                } else {
                    collection.withWriteConcern(writeConcernToUse).insertMany(documents);
                }

                return null;
            });
            return MappedDocument.toIds(documents);
        }
    }


    public static void InsertDocumentsInParallel(
            MongoClient mongoClientExtension,
            String dbName,
            String collectionName,
            String partitionKey,
            List<Document> docs) throws InterruptedException, IOException {

        if(docs==null || docs.size()<=0) {
            return;
        }

        //Sample the document to figure out number of RUS required for a document
        Document samplingDoc=docs.get(0);
        List<Document> sampleList=new ArrayList<>();
        sampleList.add(samplingDoc);
        List<InsertDocumentRunnable> insertTasks=InsertOneInParallel(mongoClientExtension,dbName,collectionName,partitionKey,sampleList);
        WaitUntilAllInsertionComplete(insertTasks);
        ValidateInsertResult(insertTasks,0);

        double rusChargedForASingleDoc= GetLatestOperationRus(mongoClientExtension,dbName).GetRus();

        //Remove the sampled document
        docs.remove(0);

        if(docs==null || docs.size()<=0) {
            return;
        }

        // TODO, get RUS from the cosmosDB instead of hard coding and make sure it doesn't t, for now it's 4000
        int cosmosDBProvisionedRus=4000;
        int batchSize=docs.size();
        if(rusChargedForASingleDoc<cosmosDBProvisionedRus) {
            batchSize = (int) (4000 / rusChargedForASingleDoc);
        }
       List<List<Document>> batches= Lists.partition(docs, batchSize);

        for(int i=0;i<batches.size();i++)
        {
            List<InsertDocumentRunnable> insertBatchTasks=InsertOneInParallel(mongoClientExtension,dbName,collectionName,partitionKey,batches.get(i));
            WaitUntilAllInsertionComplete(insertBatchTasks);
            ValidateInsertResult(insertBatchTasks,0);
        }

    }
    public static List<InsertDocumentRunnable> InsertOneInParallel(
            MongoClient mongoClientExtension,
            String dbName,
            String collectionName,
            String partitionKey,
            List<Document> docs)
    {

        int numberOfThreads=docs.size();
        List<InsertDocumentRunnable> threads=new ArrayList<InsertDocumentRunnable>(numberOfThreads);
        for(int i=0;i<numberOfThreads;i++)
        {
            InsertDocumentRunnable insertDocumentRunnable= new InsertDocumentRunnable(
                    mongoClientExtension,
                    docs.get(i),
                    dbName,
                    collectionName,
                    partitionKey);
            Thread t = new Thread(insertDocumentRunnable);
            threads.add(insertDocumentRunnable);
            t.start();
        }

        return threads;
    }


    private static void WaitUntilAllInsertionComplete(List<InsertDocumentRunnable> tasks) throws InterruptedException {
        boolean isCompleted=false;
        final long startTime = System.currentTimeMillis();
        while(!isCompleted)
        {
            isCompleted=true;
            for(int i=0;i<tasks.size();i++)
            {
                if(tasks.get(i).IsRunning())
                {
                    isCompleted=false;
                }
            }
            if(isCompleted)
            {
                Thread.sleep(1);
            }
        }
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Execution time in milli seconds: " + totalTime);
        System.out.println("Execution time in seconds: " + totalTime / 1000);
    }

    private static void ValidateInsertResult(
            List<InsertDocumentRunnable> tasks,
            int runId) throws IOException {
        boolean anyFailures=false;
        for(int i=0;i<tasks.size();i++)
        {
            if(!tasks.get(i).GetIsSucceeded())
            {
                anyFailures=true;
            }
        }
        if(anyFailures)
        {
            System.out.println("There are failures while inserting the documents");
            String fileName=String.format("Errors%d.txt",runId);
            File file = new File(fileName);
            FileWriter writer = new FileWriter(file);
            for(int i=0;i<tasks.size();i++)
            {
                if(!tasks.get(i).GetIsSucceeded())
                {
                    writer.write("===Start of failed doc===");
                    writer.write(System.lineSeparator());
                    writer.write(tasks.get(i).GetDocToInsert().toJson());
                    writer.write(System.lineSeparator());
                    writer.write("===Errors===");
                    writer.write(System.lineSeparator());
                    List<String> failedDocsError=tasks.get(i).GetErrorMessages();
                    for(int k=0;k<failedDocsError.size();k++)
                    {
                        writer.write(failedDocsError.get(k));
                        writer.write(System.lineSeparator());
                    }
                    writer.write(System.lineSeparator());
                    writer.write("===Errors End===");
                    writer.write(System.lineSeparator());
                    writer.write("===End Doc===");
                }
            }
            writer.close();
            System.out.println("Failed Document and errors available in file: "+fileName);
        }
        else
        {
            System.out.println("All documents inserted successfully.");
        }
    }
    public static RuCharge GetLatestOperationRus(MongoClient mongoClient, String dbName)
    {
        BsonDocument cmd = new BsonDocument();
        cmd.append("getLastRequestStatistics", new BsonInt32(1));
        Document requestChargeResponse = mongoClient.getDatabase(dbName).runCommand(cmd);
        if(requestChargeResponse.containsKey("RequestCharge"))
        {
            return new RuCharge(
                    requestChargeResponse.getString("CommandName"),
                    requestChargeResponse.getDouble("RequestCharge"));
        }
        return null;
    }


}
