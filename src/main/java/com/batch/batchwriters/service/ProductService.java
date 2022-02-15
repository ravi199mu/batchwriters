package com.batch.batchwriters.service;

import com.batch.batchwriters.model.Product;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class ProductService {

    public Product getProduct(){
        String url = "http://localhost:9009/product";
        RestTemplate restTemplate = new RestTemplate();
        return restTemplate.getForObject(url,Product.class);
    }
}
