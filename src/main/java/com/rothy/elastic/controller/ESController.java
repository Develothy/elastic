package com.rothy.elastic.controller;

import com.rothy.elastic.service.ESService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
public class ESController {

    @Autowired
    private ESService esService;


    @PostMapping("/{indexName}")
    public @ResponseBody String createIndex (@PathVariable String indexName) throws IOException {

        return esService.createIndexWithNori(indexName);
    }

    @GetMapping("/search")
    public @ResponseBody String search (@RequestParam String field, @RequestParam String q) throws IOException {

        return esService.getSearchResult(field, q);
    }

    @PostMapping("/{indexName}/{id}")
    public @ResponseBody String add (@PathVariable String indexName, @PathVariable Long id, @RequestBody Map<String, Object> source) throws IOException {

        return esService.insertDocument(indexName, id, source);
    }
    @PostMapping("/{indexName}/bulk")
    public @ResponseBody String bulk (@PathVariable String indexName, @RequestBody List<Map<String, Object>> source) throws IOException {

        return esService.insertBulk(indexName, source);
    }

}

