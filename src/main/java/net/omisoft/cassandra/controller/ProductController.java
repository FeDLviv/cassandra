package net.omisoft.cassandra.controller;

import lombok.AllArgsConstructor;
import net.omisoft.cassandra.dao.ProductRepository;
import net.omisoft.cassandra.model.Product;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@RestController
@AllArgsConstructor
public class ProductController {

    @Autowired
    private final ProductRepository repository;

    @RequestMapping(value = "/products", method = RequestMethod.GET)
    public Iterable<Product> getProducts() {
        return repository.findAll();
    }

    @RequestMapping(value = "/products/{name}", method = RequestMethod.POST)
    public void setProduct(@PathVariable String name) {
        repository.save(new Product(UUID.randomUUID(), name, ThreadLocalRandom.current().nextInt(1, 100)));
    }

}