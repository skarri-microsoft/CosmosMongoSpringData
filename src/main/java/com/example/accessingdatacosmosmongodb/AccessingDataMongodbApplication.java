package com.example.accessingdatacosmosmongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory;

import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class AccessingDataMongodbApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(AccessingDataMongodbApplication.class, args);
    }
    @Override
    public void run(String... args) throws Exception {



        MongoClient mongoClient = new MongoClient(
                new MongoClientURI(
                        "mongodb://your 36 URI"));

        //32
//        MongoClient mongoClient = new MongoClient(
//                new MongoClientURI(
//                        "mongodb://your 32 URI"));

        CustomMongoTemplate customMongoTemplate=
                new CustomMongoTemplate(mongoClient,"test");

        MongoRepositoryFactory factory = new MongoRepositoryFactory(customMongoTemplate);
        CustomerRepository repository2 = factory.getRepository(CustomerRepository.class);

        repository2.deleteAll();

        List<Customer> customers=new ArrayList<>();
        for(int i=0;i<4000;i++) {
            customers.add(new Customer("First Name", "Last Name"));
        }

        repository2.saveAll(customers);

        //repository2.save(new Customer("Srini","Karri"));

    }
}
