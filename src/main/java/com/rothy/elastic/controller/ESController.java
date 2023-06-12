package com.rothy.elastic.controller;

import com.rothy.elastic.service.ESService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;

@RestController
public class ESController {

    @Autowired
    private ESService esService;


    @PostMapping("/create/{indexName}")
    public @ResponseBody String createIndexWithNori(@PathVariable String indexName) throws IOException {

        return esService.createIndexWithNori(indexName);
    }

    @GetMapping("/search")
    public @ResponseBody String getSearchResult(@RequestParam String field, @RequestParam String q) throws IOException {

        return esService.getSearchResult(field, q);
    }

    @PostMapping("/insert/{id}")
    public @ResponseBody String getInsertResult(@PathVariable Long id, @RequestBody Map<String, Object> source) throws IOException {

        return esService.insertDocument(id, source);
    }

}

