package com.mysite.sbb;

import org.springframework.http.*;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

// ChatController.java
@RestController
@RequestMapping("/chatbot")
public class ChatController {

    private final RestTemplate restTemplate = new RestTemplate();

    @PostMapping("/api")
    public ResponseEntity<Map<String, String>> getChatbotReply(@RequestBody Map<String, String> payload) {
        String message = payload.get("message");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, String> request = new HashMap<>();
        request.put("message", message);

        HttpEntity<Map<String, String>> entity = new HttpEntity<>(request, headers);

        // FastAPI 서버 주소
        String fastApiUrl = "http://ml-backend:8000/api/chatbot";

        try {
            ResponseEntity<Map> response = restTemplate.postForEntity(fastApiUrl, entity, Map.class);
            String reply = (String) response.getBody().get("reply");

            Map<String, String> result = new HashMap<>();
            result.put("reply", reply);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            System.out.println(e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("reply", "FastAPI 서버와의 연결에 실패했습니다."));
        }
    }
}
