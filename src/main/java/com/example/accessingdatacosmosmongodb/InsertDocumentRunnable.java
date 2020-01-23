package com.example.accessingdatacosmosmongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class InsertDocumentRunnable implements Runnable {

    private String pkey;
    private long id;
    private List<Document> sourceDocuments;
    private Document docToInsert;
    private List<String> ErrorMessages = new ArrayList<>();
    private int defaultRetriesForThrottles = 30;
    private String dbName;
    private String collectionName;
    private MongoClient mongoClientExtension;
    private boolean isBatch;
    private int MaxWaitInMilliSeconds = 500;
    private int MinWaitInMilliSeconds = 100;
    private boolean isThrottled = false;
    private boolean isSucceeded = false;
    private boolean isRunning = true;


    public InsertDocumentRunnable(
            MongoClient mongoClientExtension,
            List<Document> documents,
            String dbName,
            String collectionName,
            String pkey,
            Boolean isBatch) {
        this.mongoClientExtension = mongoClientExtension;
        this.sourceDocuments = documents;
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.pkey = pkey;
        this.isBatch = isBatch;
    }

    public InsertDocumentRunnable(
            MongoClient mongoClientExtension,
            Document docToInsert,
            String dbName,
            String collectionName,
            String pkey) {
        this.mongoClientExtension = mongoClientExtension;
        this.docToInsert = docToInsert;
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.pkey = pkey;
        this.isBatch = false;
    }

    public List<Document> GetProcessedDocument() {
        return this.sourceDocuments;
    }

    public Document GetDocToInsert() {

        return docToInsert;
    }

    public boolean IsRunning() {
        return this.isRunning;
    }

    public boolean GetIsSucceeded() {
        return this.isSucceeded;
    }

    public List<String> GetErrorMessages() {
        return this.ErrorMessages;
    }



    private void ExecuteInsertOne(Document docToInsert) throws InterruptedException {
        int throttleRetries=0;

        while(throttleRetries<defaultRetriesForThrottles)
        {
            try
            {
                this.mongoClientExtension.getDatabase(this.dbName).getCollection(this.collectionName).insertOne(docToInsert);
                isSucceeded=true;
                break;
            }
            catch (MongoCommandException mongoCommandException)
            {
                if(ErrorHandler.IsThrottle(mongoCommandException))
                {
                    throttleRetries++;
                    isThrottled=true;

                }
                else
                {
                    ErrorMessages.add(mongoCommandException.toString());
                    break;
                }
            }
            catch (Exception ex)
            {
                if(ErrorHandler.IsThrottle(ex.getMessage()))
                {
                    throttleRetries++;
                    isThrottled=true;
                }
                else
                {
                    ErrorMessages.add(ex.getMessage());
                    break;
                }
            }
            if(isThrottled)
            {
                System.out.print("Throttled on thread id: "+id);
                Thread.sleep(new Random().nextInt(MaxWaitInMilliSeconds - MinWaitInMilliSeconds + 1) + MinWaitInMilliSeconds);
            }
        }
    }

    public void run() {

        this.id = Thread.currentThread().getId();

            try {
                ExecuteInsertOne(this.docToInsert);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        this.isRunning=false;
    }


}
