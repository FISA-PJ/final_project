package com.mysite.applyhome;

import com.mysite.applyhome.housingSubscriptionEligibility.HousingSubscriptionEligibilityService;
import com.mysite.applyhome.notice.Notice;
import com.mysite.applyhome.notice.NoticeService;
import com.mysite.applyhome.user.SiteUserDetails;
import org.springframework.http.*;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// ChatController.java
@Controller
@RequestMapping("/chatbot")
public class ChatController {

    private final RestTemplate restTemplate = new RestTemplate();
    private final NoticeService noticeService;
    private final HousingSubscriptionEligibilityService eligibilityService;

    public ChatController(NoticeService noticeService, HousingSubscriptionEligibilityService eligibilityService) {
        this.noticeService = noticeService;
        this.eligibilityService = eligibilityService;
    }

    @GetMapping({"", "/"})
    public String chatbot(Model model, @AuthenticationPrincipal SiteUserDetails userDetails) {
        if (userDetails == null) {
            return "redirect:/user/login";
        }

        String primeType = eligibilityService.getEligibilityPrimeType(userDetails.getUser());
        List<Notice> customNotices = noticeService.getNoticesByPrimeType(primeType, 0).getContent();
        
        List<Map<String, String>> noticeList = customNotices.stream()
            .map(notice -> {
                Map<String, String> noticeMap = new HashMap<>();
                noticeMap.put("noticeNumber", notice.getNoticeNumber());
                noticeMap.put("noticeTitle", notice.getNoticeTitle());
                return noticeMap;
            })
            .collect(Collectors.toList());

        model.addAttribute("notices", noticeList);
        model.addAttribute("userName", userDetails.getUser().getPersonalProfiles().getName());
        model.addAttribute("primeType", primeType);
        
        return "chatbot";
    }

    @PostMapping("/api")
    public ResponseEntity<Map<String, String>> getChatbotReply(
            @RequestBody Map<String, String> payload,
            @AuthenticationPrincipal SiteUserDetails userDetails) {
        
        String message = payload.get("message");
        String selectedNoticeNumber = payload.get("noticeNumber");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> request = new HashMap<>();
        request.put("message", message);
        request.put("noticeNumber", selectedNoticeNumber);

        // Create nested user_info object
        Map<String, String> userInfo = new HashMap<>();
        
        if (userDetails != null && userDetails.getUser() != null && userDetails.getUser().getPersonalProfiles() != null) {
            userInfo.put("registrationNumber", userDetails.getUser().getPersonalProfiles().getResidentRegistrationNumber());
            userInfo.put("userName", userDetails.getUser().getPersonalProfiles().getPersonalName());
            userInfo.put("primeType", eligibilityService.getEligibilityPrimeType(userDetails.getUser()));
            userInfo.put("subType", eligibilityService.getEligibilitySubType(userDetails.getUser()));
        } else {
            // userDetails가 null인 경우 모든 필드를 빈 문자열로 설정
            userInfo.put("registrationNumber", "");
            userInfo.put("userName", "");
            userInfo.put("primeType", "");
            userInfo.put("subType", "");
        }
        
        System.out.println(userInfo);
        
        request.put("user_info", userInfo);

        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);
        System.out.println(entity);

        // FastAPI 서버 주소
        String fastApiUrl = "http://localhost:8000/api/chatbot";

        try {
            ResponseEntity<Map> response = restTemplate.postForEntity(fastApiUrl, entity, Map.class);
            String reply = (String) response.getBody().get("reply");

            Map<String, String> result = new HashMap<>();
            result.put("reply", reply);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("reply", "FastAPI 서버와의 연결에 실패했습니다."));
        }
    }

    @GetMapping("/check-auth")
    @ResponseBody
    public Map<String, Boolean> checkAuth(@AuthenticationPrincipal SiteUserDetails userDetails) {
        Map<String, Boolean> response = new HashMap<>();
        response.put("authenticated", userDetails != null);
        return response;
    }

    @GetMapping("/notices")
    @ResponseBody
    public List<Map<String, String>> getNotices(@AuthenticationPrincipal SiteUserDetails userDetails) {
        if (userDetails == null) {
            return new ArrayList<>();
        }

        String primeType = eligibilityService.getEligibilityPrimeType(userDetails.getUser());
        List<Notice> customNotices = noticeService.getNoticesByPrimeType(primeType, 0).getContent();
        
        return customNotices.stream()
            .map(notice -> {
                Map<String, String> noticeMap = new HashMap<>();
                noticeMap.put("noticeNumber", notice.getNoticeNumber());
                noticeMap.put("noticeTitle", notice.getNoticeTitle());
                return noticeMap;
            })
            .collect(Collectors.toList());
    }
}
